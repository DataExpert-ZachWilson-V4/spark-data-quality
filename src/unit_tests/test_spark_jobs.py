import pytest
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict
from datetime import datetime
from src.jobs.job_1 import job_1
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .master("local") \
        .appName("pytest") \
        .getOrCreate()

def to_dt(date_str: str, is_date: bool = False) -> datetime:
    if is_date:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def test_job_1(spark_session) -> None:
    web_events = [
        {"user_id": 1919317753, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:38"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:39"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:40"},
        {"user_id": 1919317753, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:41"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:41"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:42"},
        {"user_id": 1919317753, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:43"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:43"}
    ]

    web_events_df: DataFrame = spark_session.createDataFrame(web_events)
    web_events_df.createOrReplaceTempView("sample_web_events")

    devices = [
        {"device_id": 532630305, "browser_type": "Other", "os_type": "Other", "device_type": "Other"},
        {"device_id": -189493684, "browser_type": "Chrome", "os_type": "Windows", "device_type": "Other"}
    ]

    devices_df: DataFrame = spark_session.createDataFrame(devices)
    devices_df.createOrReplaceTempView("sample_devices")

    event_dt = to_dt('2023-01-14', is_date=True)
    user_devices = [
        {"user_id": 1919317753, "browser_type": "Other", "dates_active": [event_dt], "date": event_dt},
        {"user_id": 1919317753, "browser_type": "Chrome", "dates_active": [event_dt], "date": event_dt},
    ]
    schema = "user_id: bigint, browser_type: string, dates_active: array<date>, date: date"
    expected_user_devices_df: DataFrame = spark_session.createDataFrame(user_devices, schema)
    expected_user_devices_df.createOrReplaceTempView("sample_user_devices")

    actual_user_devices_df = job_1(spark_session, "sample_web_events", "sample_devices", "sample_user_devices", '2023-01-14')

    assert actual_user_devices_df.collect() == expected_user_devices_df.collect()