import argparse
import asyncio
import pickle
from getpass import getpass
from pathlib import Path

from pyspark.sql import SparkSession

from pylitterbot import Account

from parse_logs import parse_events
from detections import high_usage_detection, weight_trajectory

CACHE_FILE = Path("raw_events.pkl")

spark = SparkSession.builder.appName("whisker-api-detections").getOrCreate()

"""
Modified from https://github.com/natekspencer/pylitterbot
"""
async def fetch_events(historical_log_count=1000000):
    username = getpass("Username:")
    password = getpass("Password:")

    account = Account()
    raw_events = []

    try:
        await account.connect(username=username, password=password, load_robots=True)

        for robot in account.robots:
            print(robot)
            for activity in await robot.get_activity_history(historical_log_count):
                raw_events.append(activity)
    finally:
        await account.disconnect()

    return raw_events


async def main(refresh=False):
    if refresh or not CACHE_FILE.exists():
        fetched = await fetch_events()

        if CACHE_FILE.exists():
            existing = pickle.loads(CACHE_FILE.read_bytes())
            existing_timestamps = {e.timestamp for e in existing}
            merged = existing + [e for e in fetched if e.timestamp not in existing_timestamps]
        else:
            merged = fetched

        CACHE_FILE.write_bytes(pickle.dumps(merged))
        raw_events = merged
    else:
        raw_events = pickle.loads(CACHE_FILE.read_bytes())

    events_df = parse_events(raw_events, spark)
    weight_trajectory(events_df).show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--refresh", action="store_true", help="Fetch new events and append to cache")
    args = parser.parse_args()

    asyncio.run(main(refresh=args.refresh))
