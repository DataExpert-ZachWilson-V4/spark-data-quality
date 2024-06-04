from collections import namedtuple

from chispa import assert_df_equality
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

from src.jobs.job_1 import job_1
from tests.conftest import spark_session


# Ref: review lab day 3
def test_game_dedup(spark_session):
    """
    Test for job1 dedup nba_game_details
    """

    game_detail = namedtuple(
        "game_detail",
        "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct"
    )

    # contains duplicates
    input_data = [
        game_detail(
            game_id=11000076,
            team_id=1610612738,
            team_abbreviation='NYK',
            team_city='New York',
            player_id=201186,
            player_name='John Doe',
            nickname='JD',
            start_position='Forward',
            comment='Great game!',
            min="35:20",
            fgm=1.0,
            fga=20.0,
            fg_pct=50.0
        ),
        game_detail(
            game_id=11000076,
            team_id=1610612738,
            team_abbreviation='NYK',
            team_city='New York',
            player_id=201186,
            player_name='John Doe',
            nickname='JD',
            start_position='Forward',
            comment='Great game!',
            min="35:20",
            fgm=1.0,
            fga=20.0,
            fg_pct=50.0
        )
    ]
    expected_output = [
        game_detail(
            game_id=11000076,
            team_id=1610612738,
            team_abbreviation='NYK',
            team_city='New York',
            player_id=201186,
            player_name='John Doe',
            nickname='JD',
            start_position='Forward',
            comment='Great game!',
            min="35:20",
            fgm=1.0,
            fga=20.0,
            fg_pct=50.0
        )
    ]

    # pyspark schema
    game_detail_schema = StructType(
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

    input_df = spark_session.createDataFrame(input_data, schema=game_detail_schema)
    # input_df.createOrReplaceTempView("game_details")
    expected_output_df = spark_session.createDataFrame(expected_output, schema=game_detail_schema)

    actual_df = job_1(spark_session, "bootcamp.nba_game_details")

    assert_df_equality(expected_output_df, actual_df, ignore_nullable=True)
