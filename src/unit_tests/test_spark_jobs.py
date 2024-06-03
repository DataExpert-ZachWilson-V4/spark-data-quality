import pytest
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, IntegerType
from typing import Any
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

def to_dt(date_str: str, is_date: bool = False) -> datetime:
    if is_date:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    else:
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")

def convert_row_to_tuple(row: Any) -> tuple:
    row_dict = row.asDict()
    row_dict['films'] = tuple(tuple(film) for film in row_dict['films'])
    return tuple(row_dict.values())

def test_job_1(spark_session) -> None:
    web_events = [
        {"user_id": 1919317753, "device_id": 532630305, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:38"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:39"},
        {"user_id": 1919317753, "device_id": -189493684, "host": "www.zachwilson.tech", "event_time": "2023-01-14 14:44:40"},
        {"user_id": 1919317753, "device_id": 532630305, "host": "admin.zachwilson.tech", "event_time": "2023-01-14 14:44:41"},
    ]
    we_df: DataFrame = spark_session.createDataFrame(web_events)
    we_df.createOrReplaceTempView("mock_events")
    devices = [
        {"device_id": 532630305, "browser_type": "Other", "os_type": "Other", "device_type": "Other"},
        {"device_id": -189493684, "browser_type": "Chrome", "os_type": "Windows", "device_type": "Other"}
    ]
    d_df: DataFrame = spark_session.createDataFrame(devices)
    d_df.createOrReplaceTempView("mock_devices")
    event_dt = to_dt('2023-01-14', is_date=True)
    uds = [
        {"user_id": 1919317753, "browser_type": "Other", "dates_active": [event_dt], "date": event_dt},
        {"user_id": 1919317753, "browser_type": "Chrome", "dates_active": [event_dt], "date": event_dt},
    ]
    schema = "user_id: bigint, browser_type: string, dates_active: array<date>, date: date"
    eud_df: DataFrame = spark_session.createDataFrame(uds, schema)
    eud_df.createOrReplaceTempView("mock_user_devices")
    aud_df = job_1(
        spark_session,
        "mock_events",
        "mock_devices",
        "mock_user_devices",
        '2023-01-14'
    )
    assert aud_df.collect() == eud_df.collect()

def test_job_2(spark_session: SparkSession) -> None:
    actor_films = [
        {"actor": "Milton Berle", "actor_id": "nm0000926", "film": "Little Lord Fauntleroy", "year": 1921, "votes": 283, "rating": 6.7, "film_id": "tt0012397"},
        {"actor": "Harold Lloyd", "actor_id": "nm0516001", "film": "A Sailor-Made Man", "year": 1921, "votes": 972, "rating": 6.9, "film_id": "tt0012642"},
    ]
    actors_history = [
        {"actor_id": "nm0000926", "actor": "Milton Berle", "films": [["tt0012397", "Little Lord Fauntleroy", 1921, 283, 6.7]], "quality_class": "average", "is_active": True, "current_year": 1921},
        {"actor_id": "nm0516001", "actor": "Harold Lloyd", "films": [["tt0012642", "A Sailor-Made Man", 1921, 972, 6.9]], "quality_class": "average", "is_active": True, "current_year": 1921},
    ]

    af_df = spark_session.createDataFrame(actor_films)
    af_df.createOrReplaceTempView("mock_actor_films")

    schema = StructType([
        StructField("actor_id", StringType(), True),
        StructField("actor", StringType(), True),
        StructField("films", ArrayType(ArrayType(StringType())), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", IntegerType(), True)
    ])
    eah_df: DataFrame = spark_session.createDataFrame(actors_history, schema)
    aah_df = job_2(spark_session, "mock_actor_films")
    expected_set = set([convert_row_to_tuple(row) for row in eah_df.collect()])
    actual_set = set([convert_row_to_tuple(row) for row in aah_df.collect()])

    assert expected_set == actual_set, \
        f"Actor history mismatch. \nExpected: {eah_df.collect()} \nActual: {aah_df.collect()}"

if __name__ == "__main__":
    pytest.main([__file__])
