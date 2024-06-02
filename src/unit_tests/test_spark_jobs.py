from chipsa.dataframe_comparer import *
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple
from datetime import date


# define named tuples
actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")

def test_job_1(spark):
    input_data = [
        actor_films("Wesley Snipes", "nm0000648", "The Art of War", "2000", "28821", "5.7", "tt0160009"),
        actor_films("Sean Connery", "nm0000125", "Finding Forrester", "2000", "83472", "7.3", "tt0181536"),
        actor_films("Kevin Bacon", "nm0000102", "Hollow Man", "2000", "124335", "5.8", "tt0164052"),
    ]

    # expected output based on our input
    expected_output = [
        actors("Wesley Snipes", "nm0000648", [["The Art of War", 28821, 5.7, "tt0160009", 2000 ]], "bad", true, 2000),
        actors("Sean Connery", "nm0000125", [["Finding Forrester", 83472, 7.3, "tt0181536", 2000]], "good", true,  2000),
        actors("Kevin Bacon", "nm0000102", [["Hollow Man", 124355, 5.8, "tt0164052", 2000]], "average", true, 2000)
    ]

    input_data_df = spark.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("actor_films")

    expected_output_df = spark.createDataFrame(expected_output)

    # running the job
    actual_df = job_1(spark, "actors")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)


# define named tuples
web_event = namedtuple("web_event", "user_id device_id referrer host url event_time")
device = namedtuple("device", "device_id browser_type os_type device_type")
web_event_summary = namedtuple("web_event_summary", "user_id browser_type event_date event_count")

def test_job_2(spark):
    web_input_data = [
        web_event(216906732, -227950717, None, "www.zachwilson.tech", "/?author=11", "2023-01-01 21:29:03.519 UTC"),
        web_event(216906732, -227950717, None, "www.zachwilson.tech", "/?author=12", "2023-01-01 21:29:05.112 UTC"),
        web_event(216906732, -227950717, None, "www.zachwilson.tech", "/?author=13", "2023-01-01 21:29:06.444 UTC"),
        web_event(-587627586, 1088283544, "https://www.eczachly.com/blog/life-of-a-silicon-valley-big-data-engineer-3-critical-soft-skills-for-success", "www.eczachly.com", "/search", "2023-01-01 00:01:39.907 UTC"),
        web_event(1078042172, 1088283544, "https://www.eczachly.com/blog/life-of-a-silicon-valley-big-data-engineer-3-critical-soft-skills-for-success", "www.eczachly.com", "/", "2023-01-01 00:03:24.519 UTC"),
        web_event(2029335291, 1648150437, "https://www.google.com/", "www.zachwilson.tech", "/", "2023-01-01 00:05:29.129 UTC")
    ]

    device_input_data = [
        device(-227950717, "Apache-HttpClient", "Other", "Other"),
        device(1088283544, "PetalBot", "Android", "Generic Smartphone"),
        device(1648150437, "Chrome", "Mac OS X", "Other")
    ]

    # expected output based on our input
    expected_output = [
        web_event_summary(-1366180212, "Chrome", "2023-01-01", 1),
        web_event_summary(760642255, "Googlebot", "2023-01-01", 4),
        web_event_summary(1279106990, "YandexBot", "2023-01-01", 1),
        web_event_summary(152355108, "Googlebot", "2023-01-01", 1),
        web_event_summary(-216752106, "PetalBot", "2023-01-01", 1),
        web_event_summary(842028662, "Mobile Safari", "2023-01-01", 1)
    ]

    web_event_df = spark.createDataFrame(web_input_data)
    web_event_df.createOrReplaceTempView("web_event")

    devices_df = spark.createDataFrame(device_input_data)
    devices_df.createOrReplaceTempView("device")

    expected_output_df = spark.createDataFrame(expected_output)

    # running the job
    actual_df = job_2(spark, "web_event_summary")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
