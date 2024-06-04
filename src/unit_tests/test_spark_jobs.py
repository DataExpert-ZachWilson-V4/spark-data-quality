from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple
from datetime import date

actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id") #input table for query of job 1
actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year") #this is input and output table for query of job 1

def test_actors_table(spark):
    input_data_actor_films = [
        actor_films(
            "Christian Bale",  #tests for an actor that was already in the actors data set
            "nm0000001",
            "Batman 3",
            2024,
            8090000,
            8.5,
            "tt0000003",
        ),
        actor_films(
            "Richard Garth",  #test for an up and coming actor that is not in the actors data set yet
            "nm10101010",
            "Superman: The Remake",
            2024,
            11111111111,
            10.0,
            "tt8324575",
        ),
        actor_films(
            "Douglas Adams",
            "nm0007412",
            "The HitchHicker's Guide To The Galaxy",
            2025,           # this tests for a movie out of the date range in the query
            5421052,
            4.2,
            "tt42424242",
        ),
    ]
    actor_films_data = spark.createDataFrame(input_data_actor_films)
    actor_films_data.createOrReplaceTempView("actor_films")

    input_data_actors = [
        actor(
            "Christian Bale",
            "nm0000001",
            [
                Row(
                    film="Batman 1",
                    votes=12234500,
                    rating=7.0,
                    film_id="tt0000001",
                ),
                Row(
                    film="Batman 2",
                    votes= 100000000,
                    rating=9.9,
                    film_id="tt0000002",
                )
            ],
            "star",
            True,
            2023,
        )
    ]
    fake_actors_df = spark.createDataFrame(input_data_actors)
    fake_actors_df.createOrReplaceTempView("actors")

    # expected output based on our input
    expected_output = [
        actor(
            "Christian Bale",
            "nm0000001",
            [
                Row(
                    film="Batman 1",
                    votes=12234500,
                    rating=7.0,
                    film_id="tt0000001",
                ),
                Row(
                    film="Batman 2",
                    votes= 100000000,
                    rating=9.9,
                    film_id="tt0000002",
                ),
                Row(
                    film="Batman 3",
                    votes= 8090000,
                    rating=8.5,
                    film_id="tt0000003",
                )
            ],
            "star",
            True,
            2024,
        ),
        actor(
            "Richard Garth",
            "nm10101010",
            [
                Row(
                    film="Superman: The Remake",
                    votes=11111111111,
                    rating=10.0,
                    film_id="tt8324575",
                )
            ],
            "star",
            True,
            2024,
        ),
    ]
    expected = spark.createDataFrame(expected_output)

    # running the job
    actual = job_1(spark, "rgindallas.actors")

    # verifying that the dataframes are identical
    assert_df_equality(actual, expected, ignore_nullable=True)


devices = namedtuple("Devices", "device_id browser_type os_type device_type") #input table for query of job 2
web_events = namedtuple("WebEvents", "user_id device_id referrer host url event_time") #input table for query of job 2
devices_cumulated = namedtuple("DevicesCumulated", "user_id browser_type dates_active date") #input and output table for query of job 2

def test_devices_cumulated_table(spark):

    input_data_devices = [
        devices(123456789, "Chrome", "Windows", "Other"),
        devices(111111111, "Chrome", "Windows", "Iphone"),
        devices(567891011, "Firefox", "Ubuntu", "Android"),
    ]
    fake_devices_df = spark.createDataFrame(input_data_devices)
    fake_devices_df.createOrReplaceTempView("devices")

    input_data_web_events = [
        web_events(
            7474738479,
            123456789,
            "",
            "www.eczachly.com",
            "/",
            "2023-01-02 20:00:00.000 UTC",
        ),
        web_events(
            222222222,
            111111111,
            "",
            "www.eczachly.com",
            "/",
            "2023-01-02 10:00:27.003 UTC",
        )
    ]
    fake_web_events_df = spark.createDataFrame(input_data_web_events)
    fake_web_events_df.createOrReplaceTempView("web_events")

    input_data_devices_cumulated = [
        devices_cumulated(7474738479, "Chrome", [date(2023, 1, 1)], date(2023, 1, 1))
    ]
    devices_cumulated_df = spark.createDataFrame(input_data_devices_cumulated)
    devices_cumulated_df.createOrReplaceTempView("devices_cumulated")

    # expected output based on our input
    expected_output = [
        devices_cumulated(
            7474738479, "Chrome", [date(2023, 1, 1)], date(2023, 1, 1)
        ),
        devices_cumulated(
            7474738479, "Chrome", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)
        ),
        devices_cumulated(
            222222222, "Chrome", [date(2023, 1, 2)], date(2023, 1, 2)
        )
    ]
    expected_df = spark.createDataFrame(expected_output)

    # running the job
    actual_df = job_2(spark, "devices_cumulated")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
