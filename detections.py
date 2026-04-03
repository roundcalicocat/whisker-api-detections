from pyspark.sql import  DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date


from event_types import PET_WEIGHT_RECORDED, CAT_DETECTED, CLEAN_CYCLE_COMPLETE
from config import SETTINGS


# for ref & usage to create new dataframes
# this is the desired output for email formatting
DETECTION_SCHEMA = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("cat_name", StringType(), nullable=False),
    StructField("rule_triggered", StringType(), nullable=True),
    StructField("details", StringType(), nullable=False),
])


def correlate_to_cat(input_df, weight_event_filter=True) -> DataFrame:
    """
    Helper function to enrich with cat name and exclude "Unknown" cats.
    Filters to PET_WEIGHT_RECORDED events by default; pass weight_event_filter=False
    when the df is already aggregated and has no event_type column.
    """
    default_cat_name = F.lit("Unknown")
    for name, config in SETTINGS["cats"].items():
        min_weight = config["avg_weight"] - config["weight_range"]
        max_weight = config["avg_weight"] + config["weight_range"]
        default_cat_name = (F.when(
            F.col("value").between(min_weight, max_weight), name)
            .otherwise(default_cat_name)
        )
    df = input_df.filter(F.col("event_type") == PET_WEIGHT_RECORDED) if weight_event_filter else input_df
    return (df
        .withColumn("cat_name", default_cat_name)
        .filter(F.col("cat_name") != "Unknown")
    )


def weight_downtrend_detection(spark, input_df) -> DataFrame:
    """
    Detects downtrend in cat weight over lookback_days (minimum should be 14)
    If historical data not available, early_avg_weight, at_risk, & weight_difference will be null
    Filtering out extreme readings, like a cat suddenly dropping a pound between readings,
    to prevent messing with the average.
    This can be caused by a misreading of the weight sensor in the Litter Robot, or by a cat
    quickly stepping in/out

    Weight is rounded to the hundredths
    """
    lookback_days = SETTINGS["detections"]["lookback_days"]
    weight_drop_threshold = SETTINGS["detections"]["weight_drop_threshold"]
    weight_stddev_multiplier = SETTINGS["detections"]["weight_stddev_multiplier"]
    lookback_days_half = lookback_days // 2
    current_date = F.current_date()

    add_stats_df = (input_df
                    .filter(F.col("timestamp").between(F.date_sub(current_date, lookback_days), current_date))
                    .transform(correlate_to_cat)
                    .groupBy("cat_name")
                    .agg(F.avg("value").alias("mean"), F.stddev("value").alias("stddev"))
    )

    filtered_extremes = (input_df
                         .transform(correlate_to_cat)
                         .join(add_stats_df, on="cat_name", how="left")
                         .filter(F.abs(F.col("value") - F.col("mean")) <= weight_stddev_multiplier * F.col("stddev"))
                         .drop("mean", "stddev")
    )

    first_half = (filtered_extremes
        .filter(F.col("timestamp").between(F.date_sub(current_date, lookback_days), F.date_sub(current_date, lookback_days_half + 1)))
        .transform(correlate_to_cat)
        .groupBy("cat_name")
        .agg(F.round(F.avg("value"), 2).alias("past_average"))
    )
    second_half = (filtered_extremes
        .filter(F.col("timestamp").between(F.date_sub(current_date, lookback_days_half), F.date_sub(current_date, 1)))
        .transform(correlate_to_cat)
        .groupBy("cat_name")
        .agg(F.round(F.avg("value"), 2).alias("current_average"))
    )
    joined = (first_half
        .join(second_half, on="cat_name", how="outer")
        .withColumn("difference", F.round(F.col("past_average") - F.col("current_average"), 2))
    )

    detections = []
    for cat in joined.filter(F.col("difference") > weight_drop_threshold).toPandas().itertuples():
        detections.append(
            (date.today(),
             cat.cat_name,
             "weight_trajectory_detection",
             f"{cat.cat_name} weight changed by {cat.difference}, from {cat.past_average} to {cat.current_average}")
        )

    # if no cats above weight threshold, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)


def sudden_usage_spike_detection(spark, input_df) -> DataFrame:
    """
    Detects sudden spike (spike_visit_threshold) in usage over spike_window, 
    This period should be over a few limited hours for best results

    """
    spike_window = SETTINGS["detections"]["spike_window"]
    spike_visit_threshold = SETTINGS["detections"]["spike_visit_threshold"]
    current_date = F.current_date()

    windowed_usage = (input_df
                      .filter(F.col("timestamp") >= current_date)
                      .transform(correlate_to_cat)
                      .groupBy("cat_name", F.window(F.col("timestamp"), spike_window))
                      .agg(F.count("*").alias("activity_count"))
    )

    detections = []
    for cat in windowed_usage.filter(F.col("activity_count") > spike_visit_threshold).toPandas().itertuples():
        detections.append(
            (date.today(),
             cat.cat_name,
             "sudden_usage_spike_detection",
             f"over {spike_window}, {cat.cat_name} used the litter box {cat.activity_count} times")
        )

    # if no cats above usage threshold, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)


def upward_usage_trend_detection(spark, input_df) -> DataFrame:
    """
    Detects a gradual increase in usage using linear regression

    Even in healthy cats, some varience is expected (e.g, 2-4 visits a day),
    so looking for the nice slope linear regression produces rather
    than averaging a bunch, which can lead to false negatives
    """
    lookback_days = SETTINGS["detections"]["lookback_days"]
    usage_increase_threshold = SETTINGS["detections"]["usage_increase_threshold"]
    current_date = F.current_date()

    daily_visits = (input_df
        .filter(F.col("timestamp").between(F.date_sub(current_date, lookback_days), current_date))
        .transform(correlate_to_cat)
        .withColumn("date", F.to_date(F.col("timestamp")))
        .withColumn("day_index", F.datediff(F.col("date"), F.date_sub(current_date, lookback_days)))
        .groupBy("cat_name", "date", "day_index")
        .agg(F.count("*").alias("daily_count"))
    )

    # linear regression ref: slope = (n * sum(xy) - sum(x) * sum(y) / (n * (sum(x^2)) - (sum(x)^2))))
    slopes_by_cat = (daily_visits
        .groupBy("cat_name")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.col("day_index")).alias("sum_x"),
            F.sum(F.col("daily_count")).alias("sum_y"),
            F.sum(F.col("day_index") * F.col("daily_count")).alias("sum_xy"),
            F.sum(F.col("day_index") * F.col("day_index")).alias("sum_x^2")
        )
        .withColumn("slope", (F.col("n") * F.col("sum_xy") - F.col("sum_x") * F.col("sum_y")) / (F.col("n") * F.col("sum_x^2") - F.col("sum_x") * F.col("sum_x")))
    )


    detections = []
    for cat in slopes_by_cat.filter(F.col("slope") > usage_increase_threshold).toPandas().itertuples():
        detections.append(
            (date.today(),
             cat.cat_name,
             "upward_usage_trend_detection",
             f"{cat.cat_name} has been using the litter box more over the past {lookback_days} days")
        )

    # if no cats above usage threshold, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)


def missed_day_detection(spark, input_df) -> DataFrame:
    """
    Detects if a cat wasn't seen during the day. This should be run in the PM,
    as to avoid false positives from running in the AM

    """
    # find all named cats from conf
    all_cats = list(SETTINGS["cats"].keys())

    current_cats_seen = list(input_df
                         .filter("timestamp >= current_date()")
                         .transform(correlate_to_cat)
                         .select("cat_name").distinct()
                         .toPandas()["cat_name"]
    )

    detections = []
    for cat in [x for x in all_cats if x not in current_cats_seen]:
        detections.append(
            (date.today(), 
             cat.cat_name, 
             "missed_day_detection", 
             f"{cat.cat_name} was not seen using the litter box today")
        )

    # if no cats missing, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)


def visit_duration_anomaly_detection(spark, input_df) -> DataFrame:
    """
    Detects whether there's an anomalous visit (too long/short)
    The log events don't provide a set event for a cat leaving the box, only 
    cat in -> cleaning cycle finished. Attempting to identify sessions by subtracting
    the time difference between the full cat in -> cleaning cycle from the cleaning
    cycle delay (set in the app)
    """
    cycle_delay_seconds = SETTINGS["litter_robot"]["cycle_delay"] * 60
    lookback_days = SETTINGS["detections"]["lookback_days"]
    duration_stddev_multiplier = SETTINGS["detections"]["duration_stddev_multiplier"]

    windowed_sessions = (input_df
                         .filter(F.col("timestamp") >= F.date_sub(F.current_date(), lookback_days))
                         .withColumn("session_id", F.sum(
                             (F.col("event_type") == CAT_DETECTED).cast("int")
                         ).over(Window.orderBy("timestamp")) # no partition, we die like WARN WindowExec
                         )
                         .groupBy("session_id")
                         .agg(
                             F.min(F.when(F.col("event_type") == CAT_DETECTED, F.col("timestamp"))).alias("start_time"),
                             F.max(F.when(F.col("event_type") == CLEAN_CYCLE_COMPLETE, F.col("timestamp"))).alias("end_time"),
                             F.first(F.when(F.col("event_type") == PET_WEIGHT_RECORDED, F.col("value"))).alias("value")
                         )
                         .filter(F.col("start_time").isNotNull() & F.col("end_time").isNotNull())
    )

    cat_sessions = (windowed_sessions
                    .transform(correlate_to_cat, weight_event_filter=False)
                    .groupBy("session_id")
                    .agg(F.first("cat_name").alias("cat_name"))
    )

    all_cat_sessions = (windowed_sessions
        .join(cat_sessions, on="session_id", how="inner")
        .withColumn("session_duration", (F.unix_timestamp("end_time") - F.unix_timestamp("start_time") - cycle_delay_seconds))
    )

    cat_baseline = (all_cat_sessions
        .filter(F.col("start_time") < F.current_date())
        .groupBy("cat_name")
        .agg(
            F.avg("session_duration").alias("average_duration"),
            F.stddev("session_duration").alias("stddev_duration")
        )
    )

    anomalous_sessions = (all_cat_sessions
        .filter(F.col("start_time") >= F.current_date())
        .join(cat_baseline, on="cat_name")
        .withColumn("z_score", (F.abs(F.col("session_duration") - F.col("average_duration")) / F.col("stddev_duration")))
    )

    detections = []
    for cat in anomalous_sessions.filter(F.col("z_score") > duration_stddev_multiplier).toPandas().itertuples():
        detections.append(
            (date.today(), 
             cat.cat_name, 
             "visit_duration_anomaly_detection", 
             f"{cat.cat_name} was in the litter box for an anomalous duration of {cat.session_duration} seconds")
        )

    # if no cats missing, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)