from pyspark.sql import  DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType
from datetime import date


from event_types import PET_WEIGHT_RECORDED
from enrichments import enrich_with_cat_name
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
    return (
        input_df.filter(F.col("event_type") == PET_WEIGHT_RECORDED)
        .transform(enrich_with_cat_name)
        .filter(F.col("cat_name") != "Unknown")
    )


def weight_downtrend_detection(spark, input_df) -> DataFrame:
    """
    Detects downtrend in cat weight over weight_trajectory_days (minimum should be 14)
    If historical data not available, early_avg_weight, at_risk, & weight_difference will be null
    Filtering out extreme readings, like a cat suddenly dropping a pound between readings,
    to prevent messing with the average.
    This can be caused by a misreading of the weight sensor in the Litter Robot, or by a cat
    quickly stepping in/out

    Weight is rounded to the hundredths
    """
    weight_trajectory_days = SETTINGS["detections"]["weight_trajectory_days"]
    weight_drop_threshold = SETTINGS["detections"]["weight_drop_threshold"]
    weight_stddev_multiplier = SETTINGS["detections"]["weight_stddev_multiplier"]
    weight_trajectory_days_half = weight_trajectory_days // 2
    current_date = F.current_date()

    add_stats_df = (input_df
                    .filter(F.col("timestamp").between(F.date_sub(current_date, weight_trajectory_days)))
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
        .filter(F.col("timestamp").between(F.date_sub(current_date, weight_trajectory_days), F.date_sub(current_date, weight_trajectory_days_half + 1)))
        .transform(correlate_to_cat)
        .groupBy("cat_name")
        .agg(F.round(F.avg("value"), 2).alias("past_average"))
    )
    second_half = (filtered_extremes
        .filter(F.col("timestamp").between(F.date_sub(current_date, weight_trajectory_days_half), F.date_sub(current_date, 1)))
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
    Detects sudden spike (spike_visit_threshold) in usage over spike_window_hours, 
    This period should be over a few limited hours for best results

    """
    spike_window_hours = SETTINGS["detections"]["spike_window_hours"]
    spike_visit_threshold = SETTINGS["detections"]["spike_visit_threshold"]
    window_seconds = spike_window_hours * 3600

    windowed_usage = []

    


def upward_usage_trend_detection(spark, input_df) -> DataFrame:
    """
    Detects a gradual increase in usage over defined weeks
    """
    usage_increase_days = SETTINGS["detections"]["usage_increase_days"]
    usage_increase_threshold = SETTINGS["detections"]["usage_increase_threshold"]

    usage_increase_days_half = usage_increase_days // 2
    current_date = F.current_date()

    past_half = (input_df
        .filter(F.col("timestamp").between(F.date_sub(current_date, usage_increase_days), F.date_sub(current_date, usage_increase_days_half + 1)))
        .transform(correlate_to_cat)
        .groupBy("cat_name")
        .agg(F.count("*").alias("past_average"))
    )
    current_half = (input_df
        .filter(F.col("timestamp").between(F.date_sub(current_date, usage_increase_days_half), F.date_sub(current_date, 1)))
        .transform(correlate_to_cat)
        .groupBy("cat_name")
        .agg(F.count("*").alias("current_average"))
    )
    joined_avg = (past_half
        .join(current_half, on="cat_name", how="outer")
        .withColumn("difference", F.round(F.col("past_average") - F.col("current_average"), 2))
    )

    detections = []
    for cat in joined_avg.filter(F.col("difference") < -usage_increase_threshold).toPandas().itertuples():
        detections.append(
            (date.today(),
             cat.cat_name,
             "upward_usage_trend_detection",
             f"early_avg: {cat.past_average}, current_avg: {cat.current_average}, diff: {cat.difference}")
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