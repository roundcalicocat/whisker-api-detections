import re
from typing import Optional

from pylitterbot.enums import LitterBoxStatus
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from event_types import PET_WEIGHT_RECORDED, CLEAN_CYCLES, STATUS_TO_EVENT

SCHEMA = StructType([
    StructField("timestamp", TimestampType(), nullable=False),
    StructField("event_type", StringType(), nullable=False),
    StructField("value", DoubleType(), nullable=True),
    StructField("raw", StringType(), nullable=False),
])


def _parse_row(activity) -> Optional[tuple]:
    """
    Parse Activity object from Whisker API
    """
    try:
        action = activity.action
        raw = str(action)

        if isinstance(action, LitterBoxStatus):
            event_type = STATUS_TO_EVENT.get(action)
            if not event_type:
                return None
            return (activity.timestamp, event_type, None, raw)

        # string case: "Pet Weight Recorded: 14.12 lbs"
        if action.startswith(PET_WEIGHT_RECORDED):
            match = re.search(r"(\d+\.?\d*)\s*lbs", action)
            value = float(match.group(1)) if match else None
            return (activity.timestamp, PET_WEIGHT_RECORDED, value, raw)

        # string case: "Clean Cycles: 5597"
        if action.startswith(CLEAN_CYCLES):
            match = re.search(r"(\d+)$", action)
            value = float(match.group(1)) if match else None
            return (activity.timestamp, CLEAN_CYCLES, value, raw)

        return None
    except Exception:
        return None


def parse_events(activities, spark: SparkSession) -> DataFrame:
    """
    Parse a list of Activity objects into a Spark DataFrame
    """
    rows = [row for activity in activities if (row := _parse_row(activity)) is not None]
    return spark.createDataFrame(rows, schema=SCHEMA)
