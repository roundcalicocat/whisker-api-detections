from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from event_types import PET_WEIGHT_RECORDED
from enrichments import enrich_with_cat_name
from config import CONFIG


def _get_historical_baseline() -> DataFrame:
    return 0

def _avg_weight_by_cat(input_df, time_filter, col_name) -> DataFrame:
    return (
        input_df.filter(time_filter & (F.col("event_type") == PET_WEIGHT_RECORDED))
        .transform(enrich_with_cat_name)
        .groupBy("cat_name")
        .agg(F.round(F.avg("value"), 2).alias(col_name))
    )


def weight_trajectory(input_df) -> DataFrame:
    window_days = CONFIG["detections"]["weight_trajectory_days"]
    threshold = CONFIG["detections"]["weight_drop_threshold"]
    half = window_days // 2

    today = F.current_date()
    early = _avg_weight_by_cat(input_df, F.col("timestamp").between(F.date_sub(today, window_days), F.date_sub(today, half + 1)), "early_avg_weight")
    recent = _avg_weight_by_cat(input_df, F.col("timestamp").between(F.date_sub(today, half), F.date_sub(today, 1)), "recent_avg_weight")

    diff = F.round(F.col("recent_avg_weight") - F.col("early_avg_weight"), 2)

    return (
        early.join(recent, on="cat_name", how="outer")
        .withColumn("at_risk", (F.col("early_avg_weight") - F.col("recent_avg_weight")) > threshold)
        .withColumn("weight_difference", F.when(diff < 0, diff).otherwise(F.concat(F.lit("+"), diff.cast("string"))))
    )

def high_usage_detection(input_df, lookback_days=7) -> DataFrame:
    return 0