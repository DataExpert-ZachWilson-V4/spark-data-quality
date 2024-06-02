import sys
import os
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

# Define namedtuples
ActorFilms = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
Actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")
Film = namedtuple("Film", "film votes rating film_id year")
WebEvent = namedtuple("WebEvent", "user_id device_id referrer host url event_time")
Device = namedtuple("Device", "device_id browser_type os_type device_type")
WebEventSummary = namedtuple("WebEventSummary", "user_id browser_type event_date event_count")

def test_job_1(spark):
    input_data = [
        ActorFilms("Clark Gable", "nm0000022", "Forbidden Paradise", 1924, 127, 6.1, "tt0014925"),
        ActorFilms("Buster Keaton", "nm0000036", "Sherlock Jr.", 1924, 42948, 8.2, "tt0015324"),
        ActorFilms("Buster Keaton", "nm0000036", "The Navigator", 1924, 9186, 7.7, "tt0015163"),
        ActorFilms("Jean Arthur", "nm0000795", "The Iron Horse", 1924, 1977, 7.2, "tt0015016")
    ]

    current_year = 2024

    expected_output = [
        Actor("Clark Gable", "nm0000022", [Film("Forbidden Paradise", 127, 6.1, "tt0014925", 1924)], "average", True, current_year + 1),
        Actor("Buster Keaton", "nm0000036", [Film("Sherlock Jr.", 42948, 8.2, "tt0015324", 1924), Film("The Navigator", 9186, 7.7, "tt0015163", 1924)], "excellent", True, current_year + 1),
        Actor("Jean Arthur", "nm0000795", [Film("The Iron Horse", 1977, 7.2, "tt0015016", 1924)], "average", True, current_year + 1)
    ]

    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("actor_films")

    expected_output_dataframe = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, "actor_films")

    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)

def test_job_2(spark):
    web_event_data = [
        WebEvent(216906732, -227950717, None, "www.zachwilson.tech", "/?author=11", "2023-01-01 21:29:03.519 UTC"),
        WebEvent(216906732, -227950717, None, "www.zachwilson.tech", "/?author=12", "2023-01-01 21:29:05.112 UTC"),
        WebEvent(216906732, -227950717, None, "www.zachwilson.tech", "/?author=13", "2023-01-01 21:29:06.444 UTC"),
        WebEvent(-587627586, 1088283544, "https://www.eczachly.com/blog/life-of-a-silicon-valley-big-data-engineer-3-critical-soft-skills-for-success", "www.eczachly.com", "/search", "2023-01-01 00:01:39.907 UTC"),
        WebEvent(1078042172, 1088283544, "https://www.eczachly.com/blog/life-of-a-silicon-valley-big-data-engineer-3-critical-soft-skills-for-success", "www.eczachly.com", "/", "2023-01-01 00:03:24.519 UTC"),
        WebEvent(2029335291, 1648150437, "https://www.google.com/", "www.zachwilson.tech", "/", "2023-01-01 00:05:29.129 UTC")
    ]

    devices_data = [
        Device(-227950717, "Apache-HttpClient", "Other", "Other"),
        Device(1088283544, "PetalBot", "Android", "Generic Smartphone"),
        Device(1648150437, "Chrome", "Mac OS X", "Other")
    ]

    expected_output = [
        WebEventSummary(-1366180212, "Chrome", "2023-01-01", 1),
        WebEventSummary(760642255, "Googlebot", "2023-01-01", 4),
        WebEventSummary(1279106990, "YandexBot", "2023-01-01", 1),
        WebEventSummary(152355108, "Googlebot", "2023-01-01", 1),
        WebEventSummary(-216752106, "PetalBot", "2023-01-01", 1),
        WebEventSummary(842028662, "Mobile Safari", "2023-01-01", 1)
    ]

    web_event_df = spark.createDataFrame(web_event_data)
    web_event_df.createOrReplaceTempView("web_events")

    devices_df = spark.createDataFrame(devices_data)
    devices_df.createOrReplaceTempView("devices")

    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, "web_events")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
