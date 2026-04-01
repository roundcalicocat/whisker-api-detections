from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from event_types import PET_WEIGHT_RECORDED
from enrichments import enrich_with_cat_name
from config import SETTINGS


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


def weight_trajectory(input_df) -> DataFrame:
    """
    Detects downtrend in cat weight over weight_trajectory_days (minimum should be 14)
    If historical data not available, early_avg_weight, at_risk, & weight_difference will be null

    output cols: cat_name, early_avg_weight, recent_avg_weight, at_risk, weight_difference
    """
    window_days = SETTINGS["detections"]["weight_trajectory_days"]
    threshold = SETTINGS["detections"]["weight_drop_threshold"]
    half = window_days // 2


def sudden_usage_spike(input_df) -> DataFrame:
    """
    Detects sudden spike in usage over a certain period, defined in SETTINGS config
    This period should be over a few limited hours for best results

    """
    spike_window_hours = SETTINGS["detections"]["spike_window_hours"]
    spike_visit_threshold = SETTINGS["detections"]["spike_visit_threshold"]
    window_seconds = spike_window_hours * 3600


def night_clustering(input_df) -> DataFrame:
    return 0

def upward_usage_trend(input_df) -> DataFrame:
    return 0

def visit_freq_variance(input_df) -> DataFrame:
    return 0

def missed_day(input_df) -> DataFrame:
    """
    Detects if a cat wasn't seen during the day. This should be run in the PM,
    as to avoid false positives from running in the AM


    """
    # find all named cats from conf
    cats = []
    for cat in SETTINGS["cats"]:
        cats.append(cat)

    current_cats_seen = (input_df
                         .transform(find_cat)
                         .select("cat_name").distinct()
                         .filter("timestamp >= current_date()"))

    return (
        

    )

