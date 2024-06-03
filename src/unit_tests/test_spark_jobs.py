import sys
import pytest
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import date, datetime
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2


@pytest.fixture(scope='session')
def spark():
    return (
        SparkSession.builder
        .master("local")
        .appName("chispa")
        .getOrCreate()
    )


def test_job_1(spark):
    ActorFilm = namedtuple(
        "ActorFilm", "actor actor_id film film_id year votes rating")
    Actor = namedtuple(
        "Actor", "actor actor_id films quality_class is_active current_year")
    Film = namedtuple("Film", "film film_id year votes rating")

    actor_films_input = [
        ActorFilm("Actor1", "ActorId1", "Actor1Film1",
                  "FilmIdA1F1", 1914, 100, 8.4),
        # first film of an actor appearing in multiple films
        ActorFilm("Actor1", "ActorId1", "Actor1Film2",
                  "FilmIdA1F2", 1914, 200, 7.8),
        # second film of an actor appearing in multiple films
        ActorFilm("Actor1", "ActorId1", "Actor1Film3",
                  "FilmIdA1F3", 1915, 110, 6.3),
        # thrid film, but in different year - should be filtered out by query
        ActorFilm("Actor2", "ActorId2", "Actor2Film1",
                  "FilmIdA2F1", 1914, 150, 8.3),
        # another actor, with appearance in actors table
    ]
    actors_input = [
        Actor("Actor2", "ActorId2", [
              Film("Actor2Film2", "FilmIdA2F2", 1913, 75, 7.2)], "good", True, 1913),
        # Actor2 has already a film in previous year
        Actor("Actor3", "ActorId3", [
            Film("Actor3Film1", "FilmIdA3F1", 1913, 35, 6.5)], "average", True, 1913),
        # Actor3 is not present in actor_films input
    ]

    actor_films_df = spark.createDataFrame(actor_films_input)
    actor_films_df.createOrReplaceTempView("actor_films")
    actors_df = spark.createDataFrame(actors_input)
    actors_df.createOrReplaceTempView("actors")

    expected = [
        Actor("Actor1", "ActorId1", [Film("Actor1Film1", "FilmIdA1F1", 1914, 100, 8.4), Film(
            "Actor1Film2", "FilmIdA1F2", 1914, 200, 7.8)], "star", True, 1914),
        Actor("Actor2", "ActorId2", [Film("Actor2Film1", "FilmIdA2F1", 1914, 150, 8.3), Film(
            "Actor2Film2", "FilmIdA2F2", 1913, 75, 7.2)], "star", True, 1914),
        Actor("Actor3", "ActorId3", [
              Film("Actor3Film1", "FilmIdA3F1", 1913, 35, 6.5)], "average", False, 1914),
    ]

    expected_output_dataframe = spark.createDataFrame(expected)

    actual_df = job_1(spark, "actor_films")

    assert_df_equality(actual_df, expected_output_dataframe,
                       ignore_nullable=True)


def test_job_2(spark):
    WebEvent = namedtuple("WebEvent", "user_id device_id event_time")
    Device = namedtuple("Device", "device_id browser_type")
    UserDeviceCumulated = namedtuple(
        "UserDeviceCumulated", "user_id browser_type dates_active date")

    def _date_from_str(s: str) -> date:
        # helper function to quickly convert strings to date objects
        return datetime.strptime(s, "%Y-%m-%d").date()

    web_events_input = [
        WebEvent("User1", "Device1", "2023-01-01 13:01:02.345 UTC"),
        WebEvent("User1", "Device3", "2023-01-01 14:02:03.456 UTC"),
        WebEvent("User3", "Device5", "2023-01-01 15:06:07.891 UTC")
    ]

    devices_input = [
        Device("Device1", "Chrome"),
        Device("Device2", "Chrome"),
        Device("Device3", "Firefox"),
        Device("Device4", "Safari"),
        Device("Device5", "Firefox"),
    ]

    user_devices_cumulated_input = [
        UserDeviceCumulated("User1", "Chrome", [
                            "2022-12-27", "2022-12-30", "2022-12-31"], _date_from_str("2022-12-31")),
        UserDeviceCumulated("User1", "Firefox", [
                            "2022-12-28", "2022-12-30"], _date_from_str("2022-12-31")),
        # User1 uses 2 different devices
        UserDeviceCumulated("User2", "Safari", [
                            "2022-12-30", "2022-12-31"], _date_from_str("2022-12-31")),
        # User2 exists only in history, no new appearance on current date
    ]

    web_events_df = spark.createDataFrame(web_events_input)
    web_events_df.createOrReplaceTempView("web_events")
    devices_df = spark.createDataFrame(devices_input)
    devices_df.createOrReplaceTempView("devices")
    user_devices_cumulated_df = spark.createDataFrame(
        user_devices_cumulated_input)
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")

    expected = [
        UserDeviceCumulated("User1", "Chrome", [
            "2023-01-01", "2022-12-27", "2022-12-30", "2022-12-31"], _date_from_str("2023-01-01")),
        UserDeviceCumulated("User1", "Firefox", [
            "2023-01-01", "2022-12-28", "2022-12-30"], _date_from_str("2023-01-01")),
        UserDeviceCumulated("User2", "Safari", [
                            None, "2022-12-30", "2022-12-31"], _date_from_str("2023-01-01")),
        UserDeviceCumulated("User3", "Firefox", [
                            "2023-01-01"], _date_from_str("2023-01-01")),
    ]

    expected_output_dataframe = spark.createDataFrame(expected)

    actual_df = job_2(spark, "actor_films")

    assert_df_equality(actual_df, expected_output_dataframe,
                       ignore_nullable=True, ignore_row_order=True)
