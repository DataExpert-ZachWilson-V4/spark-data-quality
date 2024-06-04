from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date

import sys
import os
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2



def test_job_1(spark_session):
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")

    input_data = [
        actor_films("Charles Chaplin", "nm0000122", "Tillie's Punctured Romance", 1914, 3301, 6.3, "tt0004707"),
        actor_films("Harold Lloyd", "nm0516001", "The Patchwork Girl of Oz", 1914, 398, 5.5, "tt0004457"),
        actor_films("Lillian Gish", "nm0001273", "Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"),
        actor_films("Lillian Gish", "nm0001273", "Home, Sweet Home", 1914, 190, 5.8, "tt0003167")
    ]

    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("actor_films")

    expected_output = [
        actors(
            "Charles Chaplin",
            "nm0000122",
            [[1914,"Tillie's Punctured Romance",3301,6.3,"tt0004707"]],
            "average",
            True,
            1914
        ),
        actors(
            "Harold Lloyd",
            "nm0516001",
            [[1914,"The Patchwork Girl of Oz",398,5.5,"tt0004457"]],
            "bad",
            True,
            1914
        ),
        actors(
            "Lillian Gish",
            "nm0001273",
            [[1914,"Judith of Bethulia",1259,6.1,"tt0004181"],[1914,"Home, Sweet Home",190,5.8,"tt0003167"]],
            "bad",
            True,
            1914
        )
    ]

    expected_output_dataframe = spark.createDataFrame(expected_output)

    actual_df = job_1(spark, "actors")

    assert_df_equality(actual_df, expected_output_dataframe, ignore_nullable=True)


def test_job_2(spark):
    web_event = namedtuple("web_event", "user_id device_id referrer host url event_time")
    device = namedtuple("device", "device_id browser_type os_type device_type")
    user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

    web_input_data = [
        web_event(-513937576, -2052907308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2023-01-01 04:58:52.248 UTC"),
        web_event(348646037, -2012543895, None, "www.zachwilson.tech", "/", "2023-01-01 02:11:36.696 UTC"),
        web_event(-1624382001, -2012543895, None, "www.zachwilson.tech", "/robots.txt", "2023-01-01 00:19:41.549 UTC"),
        web_event(-1624382001, -2012543895, None, "www.zachwilson.tech", "/", "2023-01-01 09:38:07.298 UTC"),
        web_event(871821064, 633247885, "http://zachwilson.tech/wp-login.php", "www.zachwilson.tech", "/wp-login.php", "2023-01-01 22:15:01.166 UTC"),
        web_event(1371858807, -2052907308, "http://admin.zachwilson.tech", "admin.zachwilson.tech", "/", "2023-01-01 18:07:16.700 UTC")
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
        user_devices_cumulated(-513937576, "Chrome", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(1371858807, "Chrome", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(348646037, "Googlebot", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(-1624382001, "Googlebot", ["2023-01-01"], "2023-01-01"),
        user_devices_cumulated(871821064, "Firefox", ["2023-01-01"], "2023-01-01")
    ]

    expected_output_df = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, "user_devices_cumulated")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
