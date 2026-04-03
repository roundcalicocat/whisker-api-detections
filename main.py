import argparse
import asyncio
import pickle
from functools import reduce
from getpass import getpass
from pathlib import Path

from pyspark.sql import SparkSession

from pylitterbot import Account

from parse_logs import parse_events
from detections import (weight_downtrend_detection, sudden_usage_spike_detection,
    upward_usage_trend_detection, missed_day_detection, visit_duration_anomaly_detection)
from sender import send_detections

CACHE_FILE = Path("raw_events.pkl")

spark = SparkSession.builder.appName("whisker-api-detections").getOrCreate()

async def fetch_events(historical_log_count=1000000):
    """
    Fetch past 7 days of events from each litter robot on account
    Note: historical_log_count default made on assumption cats don't use
    the bathroom 1M+ times a week, to get max # of logs
    
    Modified from https://github.com/natekspencer/pylitterbot
    """
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


async def main(refresh=False, run_mode="daily"):
    """
    Main functionality:
        - Fetch past 7 days of litter robot(s) data (required on first run, optional on rerun)
            - Recommended to refresh at least weekly
        - Parse Activity objects to create event log dataframe
        - Detection time (default is daily)
        - Send detection details to email
    """
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

    hourly = reduce(lambda a, b: a.union(b), [
        sudden_usage_spike_detection(spark, events_df),
        visit_duration_anomaly_detection(spark, events_df),
    ])

    daily = reduce(lambda a, b: a.union(b), [
        weight_downtrend_detection(spark, events_df),
        upward_usage_trend_detection(spark, events_df),
        missed_day_detection(spark, events_df),
    ])

    email_password = getpass("Email password:")

    if run_mode == "hourly":
        send_detections(hourly, email_password)
    elif run_mode == "daily":
        send_detections(daily, email_password)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    # it's annoying to log in everytime to run this thing
    parser.add_argument("--refresh", action="store_true", help="Fetch new events and append to cache")
    parser.add_argument("--mode", choices=["hourly", "daily"], default="daily", help="Detection frequency")
    args = parser.parse_args()

    asyncio.run(main(refresh=args.refresh, run_mode=args.mode))
