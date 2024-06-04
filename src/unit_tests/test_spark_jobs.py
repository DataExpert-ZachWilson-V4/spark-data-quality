from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import date

import sys
import os
from ..jobs.job_1  import job_1
from ..jobs.job_2  import job_2


def test_job_one(spark_session):
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")
    
    input_data = [
        actor_films("Antonio Banderas", "nm0000104", "Life Itself", 2018, 18030, 6.9, "tt5989218"),
        actor_films("Antonio Banderas", "nm0000104", "Beyond the Edge", 2018, 2815, 4.5, "tt5629524"),
        actor_films("Nicolas Cage", "nm0000115", "Spider-Man: Into the Spider-Verse", 2018, 389682, 8.4, "tt4633694"),
        actor_films("Nicolas Cage", "nm0000115", "Mandy", 2018, 65757, 6.5, "tt6998518")
    ]
    
    expected_output = [
        actors(
            "Antonio Banderas",
            "nm0000104",
            [["Life Itself", 18030, 6.9, "tt5989218"], ["Beyond the Edge", 2815, 4.5, "tt5629524"]],
            "bad",
            True,
            2018
        ),
        actors(
            "Nicolas Cage",
            "nm0000115",
            [["Spider-Man: Into the Spider-Verse", 389682, 8.4, "tt4633694"], ["Mandy", 65757, 6.5, "tt6998518"]],
            "good",
            True,
            2018
        ),
        actors(
            "Erika Eleniak",
            "nm0000143",
            [["Boone: The Bounty Hunter", 2180, 5.5, "tt3229488"]],
            "bad",
            False,
            2018
        )
    ]

    last_year_output = [
        actors(
            "Erika Eleniak",
            "nm0000143",
            [["Boone: The Bounty Hunter", 2180, 5.5, "tt3229488"]],
            "bad",
            False,
            2017
        )
    ]

    input_dataframe = spark_session.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("actor_films")

    last_year_dataframe = spark_session.createDataFrame(last_year_output)
    last_year_dataframe.createOrReplaceTempView("actors")

    expected_output_dataframe = spark_session.createDataFrame(expected_output)

    actual_df = job_1(spark_session, "actors")
    
    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)


def test_job_2(spark_session):
    web_events = namedtuple("web_events", "user_id device_id referrer host url event_time")
    devices = namedtuple("devices", "device_id browser_type os_type device_type")
    user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

    web_events_input_data = [
        web_events(-1624382001, -2012543895, None, "www.zachwilson.tech", "/", "2023-01-01 09:38:07.298 UTC"),
        web_events(-1624382001, -2012543895, None, "www.zachwilson.tech", "/robots.txt", "2023-01-01 00:19:41.549 UTC"),
        web_events(1371858807, -2052907308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2023-01-01 18:07:16.700 UTC"),
        web_events(871821064, 633247885, "http://zachwilson.tech/wp-login.php", "www.zachwilson.tech", "/wp-login.php", "2023-01-01 22:15:01.166 UTC")
    ]

    web_events_df = spark_session.createDataFrame(web_events_input_data)
    web_events_df.createOrReplaceTempView("web_events")

    devices_input_data = [
        devices(-2012543895, "Googlebot", "Other", "Spider"),
        devices(-2052907308, "Chrome", "Mac OS X", "Other"),
        devices(633247885, "Firefox", "Windows", "Other")
    ]

    devices_df = spark_session.createDataFrame(devices_input_data)
    devices_df.createOrReplaceTempView("devices")
    
    
    yesterday_user_devices_cummulated = [user_devices_cumulated(149507145, "Other", ["2022-12-31"], "2022-12-31")]
    yesterday_dataframe = spark_session.createDataFrame(yesterday_user_devices_cummulated)
    yesterday_dataframe.createOrReplaceTempView("user_devices_cumulated")

    expected_output = [
        user_devices_cumulated(-1624382001, "Googlebot", ["2023-01-01"], date(2023,1,1)),
        user_devices_cumulated(149507145, "Other", [None,"2022-12-31"], date(2023,1,1)),
        user_devices_cumulated(871821064, "Firefox", ["2023-01-01"], date(2023,1,1)),
        user_devices_cumulated(1371858807, "Chrome", ["2023-01-01"], date(2023,1,1))
    ]

    expected_output_df = spark_session.createDataFrame(expected_output)

    actual_df = job_2(spark_session, "user_devices_cumulated")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
