import sys
import os
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2


def test_actors(spark):
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")
    input_data_actor_films = [
        actor_films("Christian Bale", "nm0000001", "Batman 3", 2024, 8090000, 8.5, "tt0000003"),
        actor_films("Mateo Posada", "nm10101010", "Superman: The Remake", 2024, 11111111111, 10.0, "tt8324575"),
        actor_films("Douglas Adams", "nm0007412", "The HitchHiker's Guide To The Galaxy", 2025, 5421052, 4.2, "tt42424242"),
    ]

    input_data_actors = [
        actors(
            "Christian Bale",
            "nm0000001",
            [
                ["Batman 1", 12234500, 7.0, "tt0000001"],
                ["Batman 2", 100000000, 9.9, "tt0000002"],
            ],
            "star",
            True,
            2023
        )
    ]

    expected_output = [
        actors(
            "Christian Bale",
            "nm0000001",
            [
                ["Batman 1", 12234500, 7.0, "tt0000001"],
                ["Batman 2", 100000000, 9.9, "tt0000002"],
                ["Batman 3", 8090000, 8.5, "tt0000003"],
            ],
            "star",
            True,
            2024
        ),
        actors(
            "Mateo Posada",
            "nm10101010",
            [["Superman: The Remake", 11111111111, 10.0, "tt8324575"]],
            "star",
            True,
            2024
        )
    ]

    input_dataframe_actor_films = spark.createDataFrame(input_data_actor_films)
    input_dataframe_actor_films.createOrReplaceTempView("actor_films")

    input_dataframe_actors = spark.createDataFrame(input_data_actors)
    input_dataframe_actors.createOrReplaceTempView("actors")

    expected_output_dataframe = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, "actors")

    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)


def test_devices_cumulated(spark):
    devices = namedtuple("Devices", "device_id browser_type os_type device_type")
    web_events = namedtuple("WebEvents", "user_id device_id referrer host url event_time")
    devices_cumulated = namedtuple("DevicesCumulated", "user_id browser_type dates_active date")
    input_data_devices = [
        devices(123456789, "Chrome", "Windows", "Other"),
        devices(111111111, "Chrome", "Windows", "Iphone"),
        devices(567891011, "Firefox", "Ubuntu", "Android"),
    ]
    
    input_data_web_events = [
        web_events(7474738479, 123456789, "", "www.eczachly.com", "/", "2023-01-02 20:00:00.000 UTC"),
        web_events(222222222, 111111111, "", "www.eczachly.com", "/", "2023-01-02 10:00:27.003 UTC")
    ]
    
    input_data_devices_cumulated = [
        devices_cumulated(7474738479, "Chrome", [date(2023, 1, 1)], date(2023, 1, 1))
    ]
    
    expected_output = [
        devices_cumulated(7474738479, "Chrome", [date(2023, 1, 1)], date(2023, 1, 1)),
        devices_cumulated(7474738479, "Chrome", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)),
        devices_cumulated(222222222, "Chrome", [date(2023, 1, 2)], date(2023, 1, 2))
    ]

    input_dataframe_devices = spark.createDataFrame(input_data_devices)
    input_dataframe_devices.createOrReplaceTempView("devices")

    input_dataframe_web_events = spark.createDataFrame(input_data_web_events)
    input_dataframe_web_events.createOrReplaceTempView("web_events")

    input_dataframe_devices_cumulated = spark.createDataFrame(input_data_devices_cumulated)
    input_dataframe_devices_cumulated.createOrReplaceTempView("devices_cumulated")

    expected_output_dataframe = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, "devices_cumulated")

    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)
