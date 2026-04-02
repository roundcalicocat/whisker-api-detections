from pyspark.sql import  DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date


from event_types import PET_WEIGHT_RECORDED
from config import SETTINGS


# for ref & usage to create new dataframes
# this is the desired output for email formatting
DETECTION_SCHEMA = StructType([
    StructField("date", DateType(), nullable=False),
    StructField("cat_name", StringType(), nullable=False),
    StructField("rule_triggered", StringType(), nullable=True),
    StructField("details", StringType(), nullable=False),
])


def correlate_to_cat(input_df):
    """
    Helper function to filter PET_WEIGHT_RECORDED events, enrich with cat name,
    and exclude "Unknown" cats
    """
    default_cat_name = F.lit("Unknown")
    for name, config in SETTINGS["cats"].items():
        min_weight = config["avg_weight"] - config["weight_range"]
        max_weight = config["avg_weight"] + config["weight_range"]
        default_cat_name = (F.when(
            F.col("value").between(min_weight, max_weight), name)
            .otherwise(default_cat_name)
        )
    return (input_df
        .filter(F.col("event_type") == PET_WEIGHT_RECORDED)
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
             f"early_avg: {cat.past_average}, current_avg: {cat.current_average}, diff: {cat.difference}")
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
             f"activity over {spike_window}: {cat.activity_count}")
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
             f"slope: {cat.slope}")
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
             cat, 
             "missed_day_detection", 
             "")
        )

    # if no cats missing, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)