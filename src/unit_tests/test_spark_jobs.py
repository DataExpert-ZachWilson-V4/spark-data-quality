from chispa.dataframe_comparer import *
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from pyspark.sql.types import (
    StructType,
    StructField,
    ArrayType,
    DateType,
    StringType,
    DoubleType,
    LongType,
)
from collections import namedtuple
@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local").appName("pytest-spark").getOrCreate()
    yield spark
    spark.stop()
Game = namedtuple(
    "Game",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct",
)
DeviceHistory = namedtuple(
    "DeviceHistory",
    "user_id browser_type dates_active date",
)
DeviceHistoryDateInt = namedtuple(
    "DeviceHistoryDateInt", "user_id browser_type history_int history_in_binary"
)
def test_game_details_dedupe(spark):
    input_data = [
        Game(
            20801112, 1610612758, "SAC", "Sacramento", 201150, "Spencer Hawes", None, "C", None, "35:41", 6.0, 13.0, 0.462,
        ),

        Game(
            20801112, 1610612758, "SAC", "Sacramento", 201150, "Spencer Hawes", None, "C", None, "35:41", 6.0, 13.0, 0.462,
        ),
    ]

    schema = StructType(
        [
            StructField("game_id", LongType(), True),
            StructField("team_id", LongType(), True),
            StructField("team_abbreviation", StringType(), True),
            StructField("team_city", StringType(), True),
            StructField("player_id", LongType(), True),
            StructField("player_name", StringType(), True),
            StructField("nickname", StringType(), True),
            StructField("start_position", StringType(), True),
            StructField("comment", StringType(), True),
            StructField("min", StringType(), True),
            StructField("fgm", DoubleType(), True),
            StructField("fga", DoubleType(), True),
            StructField("fg_pct", DoubleType(), True),
        ]
    )

    input_dataframe = spark.createDataFrame(input_data, schema=schema)
    input_table_name = "nba_game_details"
    input_dataframe.write.option(
        "path", spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    ).mode("overwrite").saveAsTable(input_table_name)
    actual_df = job_1(spark, "nba_game_details", "nba_game_details_dedup")
    expected_output = [
        Game(
            20801112, 1610612758, "SAC", "Sacramento", 201150, "Spencer Hawes", None, "C", None, "35:41", 6.0, 13.0, 0.462,
        )
    ]
    expected_df = spark.createDataFrame(expected_output, schema=schema)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
def test_device_cumulation(spark):
    input_data = [
        DeviceHistory(
            -1358803869,
            "YandexBot",
            [datetime.strptime("2023-01-01", "%Y-%m-%d").date()],
            datetime.strptime("2023-01-01", "%Y-%m-%d").date()
        ),
        DeviceHistory(
            -1816209818,
            "Googlebot",
            [
                datetime.strptime("2023-01-06", "%Y-%m-%d").date(),
                datetime.strptime("2023-01-05", "%Y-%m-%d").date(),
                datetime.strptime("2023-01-04", "%Y-%m-%d").date(),
                datetime.strptime("2023-01-03", "%Y-%m-%d").date(),
                datetime.strptime("2023-01-02", "%Y-%m-%d").date(),
                datetime.strptime("2023-01-01", "%Y-%m-%d").date(),
            ],
            datetime.strptime("2023-01-06", "%Y-%m-%d").date(),
        ),
        DeviceHistory(
            -2077270748,
            "Googlebot",
            [datetime.strptime("2023-01-06", "%Y-%m-%d").date()],
            datetime.strptime("2023-01-06", "%Y-%m-%d").date(),
        ),
    ]

    schema_input = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("browser_type", StringType(), True),
            StructField("dates_active", ArrayType(DateType()), True),
            StructField("date", DateType(), True),
        ]
    )

    input_dataframe = spark.createDataFrame(input_data, schema=schema_input)
    input_table_name = "device_history"
    input_dataframe.write.option(
        "path", spark.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    ).mode("overwrite").saveAsTable(input_table_name)

    actual_df = job_2(spark, "device_history", "history_date_list_int")
    expected_output = [
        DeviceHistoryDateInt(
            -2077270748,
            "Googlebot",
            2**30,
            bin(2**30)[2:],
        ),
        DeviceHistoryDateInt(
            -1816209818,
            "Googlebot",
            2**30 + 2**29 + 2**28 + 2**27 + 2**26 + 2**25,
            bin(2**30 + 2**29 + 2**28 + 2**27 + 2**26 + 2**25)[2:],
        ),
    ]
    schema_output = StructType(
        [
            StructField("user_id", LongType(), True),
            StructField("browser_type", StringType(), True),
            StructField("history_int", LongType(), True),
            StructField("history_in_binary", StringType(), True),
        ]
    )
    expected_df = spark.createDataFrame(expected_output, schema=schema_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
