
from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date

import sys
import os
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2



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
            [["Life Itself", 18030, 6.9, "tt5989218", 2018], ["Beyond the Edge", 2815, 4.5, "tt5629524", 2018]],
            "bad",
            True,
            2018
        ),
        actors(
            "Nicolas Cage",
            "nm0000115",
            [["Spider-Man: Into the Spider-Verse", 389682, 8.4, "tt4633694", 2018], ["Mandy", 65757, 6.5, "tt6998518", 2018]],
            "good",
            True,
            2018
        ),
        actors(
            "Erika Eleniak",
            "nm0000143",
            [["Boone: The Bounty Hunter", 2180, 5.5, "tt3229488", 2017]],
            "bad",
            False,
            2017
        )
    ]

    last_year_output = [
        actors(
            "Erika Eleniak",
            "nm0000143",
            [["Boone: The Bounty Hunter", 2180, 5.5, "tt3229488", 2017]],
            "bad",
            False,
            2017
        )
    ]

    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("actor_films")

    last_year_dataframe = spark.createDataFrame(last_year_output)
    last_year_dataframe.createOrReplaceTempView("actors")

    expected_output_dataframe = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, "actors")

    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)


def test_job_2(spark):
    web_event = namedtuple("web_event", "user_id device_id referrer host url event_time")
    device = namedtuple("device", "device_id browser_type os_type device_type")
    user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

    web_input_data = [
        web_event(348646037, -2012543895, None, "www.zachwilson.tech", "/", "2023-01-01 02:11:36.696 UTC"),
        web_event(-1624382001, -2012543895, None, "www.zachwilson.tech", "/", "2023-01-01 09:38:07.298 UTC"),
        web_event(-1624382001, -2012543895, None, "www.zachwilson.tech", "/robots.txt", "2023-01-01 00:19:41.549 UTC"),
        web_event(-513937576, -2052907308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2023-01-01 04:58:52.248 UTC"),
        web_event(1371858807, -2052907308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2023-01-01 18:07:16.700 UTC"),
        web_event(871821064, 633247885, "http://zachwilson.tech/wp-login.php", "www.zachwilson.tech", "/wp-login.php", "2023-01-01 22:15:01.166 UTC")
    ]

    web_event_df = spark.createDataFrame(web_input_data)
    web_event_df.createOrReplaceTempView("web_event")

    device_input_data = [
        device(-2012543895, "Googlebot", "Other", "Spider"),
        device(-2052907308, "Chrome", "Mac OS X", "Other"),
        device(633247885, "Firefox", "Windows", "Other")
    ]

    devices_df = spark.createDataFrame(device_input_data)
    devices_df.createOrReplaceTempView("device")

    expected_output = [
        user_devices_cumulated(348646037, "Googlebot", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(-1624382001, "Googlebot", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(-513937576, "Chrome", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(1371858807, "Chrome", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(871821064, "Firefox", ["2023-01-01"], "2023-01-01")
    ]

    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, "user_devices_cumulated", "2021-03-20")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
