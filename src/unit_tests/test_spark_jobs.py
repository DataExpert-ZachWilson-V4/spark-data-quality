from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple
from datetime import date

actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
actor = namedtuple("Actor", "actor_id actor films quality_class is_active current_year")
web_events = namedtuple("webEvents", "user_id device_id referrer host url event_time")
devices = namedtuple("Devices", "device_id browser_type os_type device_type")
user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

def test_job1(spark):
    input_actor_films = [
        actor_films(
            "Brad Pitt", "nm0000093", "The Assassination of Jesse James by the Coward Robert Ford", 2007, 170824, 7.5, "tt0443680"
        ),
        actor_films("Kevin Bacon", "nm0000102", "The Air I Breathe", 2007, 33479, 6.8, "tt0485851")
    ]
        

    input_actor = [
        actor("nm0000093", "Brad Pitt", [Row(film="The Curious Case of Benjamin Button",votes=595444,rating=7.8,film_id="tt0421715",)], "good", True, 2007)
    ]
    test_actor_films_df = spark.createDataFrame(input_actor_films)
    test_actor_films_df.createOrReplaceTempView("actor_films")

    test_actor_df = spark.createDataFrame(input_actor)
    test_actor_df.createOrReplaceTempView("actor")

    expected_output = [
        actor("nm0000093", "Brad Pitt", [ROW(film="The Curious Case of Benjamin Button",votes=595444,rating=7.8,film_id="tt0421715",), (film="The Assassination of Jesse James by the Coward Robert Ford",votes=170824,rating= 7.5,film_id="tt0443680",)], "good", True, 2008),
        actor("nm0000102", "Kevin Bacon", [ROW(film="The Air I Breathe",votes=33479,rating=6.8,film_id='tt0485851',)],"average",True,2008)
    ]
    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, "actors")

    assert_df_equality = (actual_df, expected_output_df, ignore_nullable=True)

def test_job2(spark):

    input_web_events =  [web_events(2073894501, 36450401, "https://eczachly.substack.com/", "www.eczachly.com", "/signup", "2023-08-25 06:06:06.721 UTC"),
    web_events(542609727, 543560539, "https://www.linkedin.com/", "www.dataengineer.io", "/", "2023-08-25 05:59:56.406 UTC")]

    input_devices = [devices(36450401, "Chrome", "Windows", "Other"), devices(543560539, "Mobile Safari UI/WKWebView", "iOS", "iPhone")]

    input_devices_cumulated = [user_devices_cumulated(2092775577, "Other", [date(2023,1,1)], date(2023,1,1)), user_devices_cumulated(2073894501, "Chrome", [date(2023,1,1)], date(2023,1,1))]

    test_web_events_df = spark.createDataFrame(input_web_events)
    test_web_events_df.createOrReplaceTempView("web_events")

    test_devices_df = spark.createDataFrame(input_devices)
    test_devices_df.createOrReplaceTempView("devices")

    test_user_devices_cumulated_df = spark.createDataFrame(input_user_devices_cumulated)
    test_user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")

    expected_output = [
        user_devices_cumulated(2092775577, "Other", [date(2023,1,1)], date(2023,1,1),),
        user_devices_cumulated(2073894501, "Chrome", [date(2023,1,1), date(2023,8,25)], date(2023,8,25),),
    ]

    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, "user_devices_cumulated")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)







