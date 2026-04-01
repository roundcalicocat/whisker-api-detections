from pyspark.sql import functions as F

from config import SETTINGS


def find_cat_name(input_df):
    """
    Enriches only PET_WEIGHT_RECORDED events with a cat_name column based on
    weight ranges in config. Events with weights outside of known ranges
    are given the cat_name "Unknown"

    prereq: input_df must be filtered to PET_WEIGHT_RECORDED events only

    output cols: input_df cols, cat_name
    """
    for name, config in reversed(SETTINGS["cats"].items()):
        # litter robot sensor is inaccurate, need to get +/- range
        min_weight = config["avg_weight"] - config["weight_range"]
        max_weight = config["avg_weight"] + config["weight_range"]

    return input_df.withColumn(
        "cat_name",
        F.when(
            # default to "unknown" if weight can't be associated
            # a cat stepping in once and immediately exiting can cause this
            F.col("value").between(min_weight, max_weight), name)
            .otherwise(F.lit("Unknown")
        ) 
    )
