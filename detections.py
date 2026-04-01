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


def find_cat(input_df):
    """
    Helper function to filter PET_WEIGHT_RECORDED events, enrich with cat name,
    and exclude "Unknown" cats
    """
    return (
        input_df.filter(F.col("event_type") == PET_WEIGHT_RECORDED)
        .transform(enrich_with_cat_name)
        .filter(F.col("cat_name") != "Unknown")
    )


def split_avg_over_lookback(input_df, lookback_days, aggregation):
    """
    Helper function to handle aggregated time-split detections that are later joined
    E.g., comparing weights between weeks, visit count between weeks
    """
    lookback_days_half = lookback_days // 2
    current_date = F.current_date()

    first_half_avg = (input_df
                      .filter(F.col("timestamp").between(F.date_sub(current_date, lookback_days), F.date_sub(current_date, lookback_days_half + 1)))
                      .transform(find_cat)
                      .groupBy("cat_name")
                      .agg(aggregation.alias("past_average"))
    )
    second_half_avg = (input_df
                       .filter(F.col("timestamp").between(F.date_sub(current_date, lookback_days_half), F.date_sub(current_date, 1)))
                       .transform(find_cat)
                       .groupBy("cat_name")
                       .agg(aggregation.alias("current_average"))
    )
    
    return (
        first_half_avg
        .join(second_half_avg, on="cat_name", how="outer")
        .withColumn("difference", F.round(F.col("past_average") - F.col("current_average"), 2))
    )


def weight_trajectory(spark, input_df) -> DataFrame:
    """
    Detects downtrend in cat weight over weight_trajectory_days (minimum should be 14)
    If historical data not available, early_avg_weight, at_risk, & weight_difference will be null

    Weight is rounded to the hundredths
    """
    weight_trajectory_days = SETTINGS["detections"]["weight_trajectory_days"]
    weight_drop_threshold = SETTINGS["detections"]["weight_drop_threshold"]

    joined_avg = split_avg_over_lookback(input_df, weight_trajectory_days, F.round(F.avg("value"), 2))

    detections = []
    # filtering on cats where the weight difference between past and current surpasses the threshold
    for cat in joined_avg.filter(F.col("difference") > weight_drop_threshold).toPandas().itertuples():
        detections.append(
            (date.today(), 
             cat.cat_name, 
             "weight_trajectory_detection", 
             f"early_avg: {cat.past_average_weight}, current_avg: {cat.current_average_weight}, diff: {cat.weight_difference}")
        )

    # if no cats with weight under threshold, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)


def sudden_usage_spike(spark, input_df) -> DataFrame:
    """
    Detects sudden spike (spike_visit_threshold) in usage over spike_window_hours, 
    This period should be over a few limited hours for best results

    """
    spike_window_hours = SETTINGS["detections"]["spike_window_hours"]
    spike_visit_threshold = SETTINGS["detections"]["spike_visit_threshold"]
    window_seconds = spike_window_hours * 3600


def night_clustering(spark, input_df) -> DataFrame:
    """
    Detects a historic clustering of activity at night, indicating a possible blockage
    """
    return 0


def upward_usage_trend(spark, input_df) -> DataFrame:
    """
    Detects a gradual increase in usage over defined weeks
    """
    usage_increase_days = SETTINGS["detections"]["usage_increase_days"]
    usage_increase_threshold = SETTINGS["detections"]["usage_increase_threshold"]

    joined_avg = split_avg_over_lookback(input_df, usage_increase_days, F.count("*"))

    detections = []
    # filtering on cats where the average difference between past and current surpasses the threshold
    for cat in joined_avg.filter(F.col("average_difference") > usage_increase_threshold).toPandas().itertuples():
        detections.append(
            (date.today(), 
             cat.cat_name, 
             "upward_usage_trend_detection", 
             f"early_avg: {cat.past_average_visits}, current_avg: {cat.current_average_visits}, diff: {cat.average_difference}")
        )

    # if no cats with weight under threshold, df will be empty
    return spark.createDataFrame(detections, DETECTION_SCHEMA)


def missed_day(spark, input_df) -> DataFrame:
    """
    Detects if a cat wasn't seen during the day. This should be run in the PM,
    as to avoid false positives from running in the AM

    """
    # find all named cats from conf
    all_cats = list(SETTINGS["cats"].keys())

    current_cats_seen = list(input_df
                         .filter("timestamp >= current_date()")
                         .transform(find_cat)
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

