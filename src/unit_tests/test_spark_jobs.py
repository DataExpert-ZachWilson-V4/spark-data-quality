import sys
import os
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date


from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")


def test_job_one(spark):
    input_data = [
        actor_films("Fred Astaire","nm0000001","On the Beach",1925,12066,7.2,"tt0053137"),
        actor_films("Buster Keaton","nm0000036","Sherlock Jr.",1925,42948,8.2,"tt0015324"),
        actor_films("Buster Keaton","nm0000036","The Navigator",1925,9186,7.7,"tt0015163"),

    ]


    expected_output = [
        actors(
        "Fred Astaire",
        "nm0000001",
        [["On the Beach", 12066,7.2,"tt0053137",1925]],
        "good",
        True,
        1925
        ),

        actors(
        "Buster Keaton",
        "nm0000036",
        [["Sherlock Jr.",42948,8.2,"tt0015324",1925],["The Navigator", 9186,7.7,"tt0015163",1925]],
        "good",
        True,
        1925
        )
        ,
        actors(
        "Uma Thurman",
        "nm0000235",
        [["Tape",18993,7.3,"tt0275719",1924],["Chelsea Walls",2008,5.2,"tt0226935",1924]],
        "average",
        False,
        1925
        )
    ]
    
    last_year_output = [
        
        actors(
        "Uma Thurman",
        "nm0000235",
        [["Tape",18993,7.3,"tt0275719",1924],["Chelsea Walls",2008,5.2,"tt0226935",1924]],
        "average",
        True,
        1924
        )
    ]

    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("actor_films")

    last_season_dataframe = spark.createDataFrame(last_year_output)
    last_season_dataframe.createOrReplaceTempView("actors")
    
    expected_output_dataframe = spark.createDataFrame(expected_output)

 
    actual_df = job_1(spark,"actors")
    
    assert_df_equality(actual_df,expected_output_dataframe,ignore_nullable=True)
    


devices = namedtuple("Devices", "device_id browser_type dates_active event_count")
web_events = namedtuple("WebEvents", "user_id device_id referrer host url event_time")
devices_cumulated = namedtuple(
    "DevicesCumulated", "user_id browser_type dates_active date"
)

def test_job_two(spark):
       
       input_data_devices = [
        devices(-1138341683, "Chrome", [date(2023, 1, 2)], 1),
        devices(1967566123, "Safari", [date(2023, 1, 2)], 2),
        devices(328474741, "Mobile Safari", [date(2022, 12, 31)], 3)
        ]
       fake_devices_df = spark.createDataFrame(input_data_devices)
       fake_devices_df.createOrReplaceTempView("devices")

       input_data_web_events = [
             
        web_events(
            1967566579,
            -1138341683,
            "",
            "www.eczachly.com",
            "/",
            "2023-01-02 21:57:37.422 UTC",
        ),     
        web_events(
            1967566580,
            1967566123,
            "",
            "www.eczachly.com",
            "/",
            "2023-01-02 21:57:37.422 UTC",
        ),
        web_events(
            1041379120,
            1967566123,
            None,
            "www.eczachly.com",
            "/lessons",
            "2023-01-02 08:01:51.009 UTC",
        ),
        web_events(
            -1041379335,
            328474741,
            "",
            "admin.zachwilson.tech",
            "/",
            "2022-12-31 08:01:51.009 UTC",
        ),
        ]
       fake_web_events_df = spark.createDataFrame(input_data_web_events)
       fake_web_events_df.createOrReplaceTempView("web_events")

       input_data_devices_cumulated = [
        devices_cumulated(1967566579, "Chrome", [date(2023, 1, 1)], date(2023, 1, 1)),
        devices_cumulated(1041379120, None, [date(2023, 1, 1)], date(2023, 1, 1)),
            ]
       devices_cumulated_df = spark.createDataFrame(input_data_devices_cumulated)
       devices_cumulated_df.createOrReplaceTempView("devices_cumulated")

       expected_output = [
        devices_cumulated(
            1041379120, "Safari", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)
        ),
        devices_cumulated(
            1967566579, "Chrome", [date(2023, 1, 2),date(2023, 1, 1)], date(2023, 1, 2)
        ),
        devices_cumulated(
            1967566580, "Safari", [date(2023, 1, 2)], date(2023, 1, 2)
        ),
        ]
       expected_df = spark.createDataFrame(expected_output)

       actual_df = job_2(spark, "devices_cumulated")
       assert_df_equality(actual_df, expected_df, ignore_nullable=True)
