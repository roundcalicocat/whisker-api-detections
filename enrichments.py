from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import CONFIG
from event_types import PET_WEIGHT_RECORDED


def enrich_with_cat_name(df: DataFrame) -> DataFrame:
    """
    Enriches PET_WEIGHT_RECORDED rows with a cat_name column based on
    weight ranges defined in config.py. Non-weight rows get null, weights
    outside all known ranges get "Unknown".
    """
    cat_expr = F.lit("Unknown")
    for name, cfg in reversed(CONFIG["cats"].items()):
        low = cfg["avg_weight"] - cfg["weight_range"]
        high = cfg["avg_weight"] + cfg["weight_range"]
        cat_expr = F.when(F.col("value").between(low, high), name).otherwise(cat_expr)

    return df.withColumn(
        "cat_name",
        F.when(F.col("event_type") == PET_WEIGHT_RECORDED, cat_expr)
    )
