from collections import namedtuple
from pyspark.sql import SparkSession
import pytest
from chispa.dataframe_comparer import assert_df_equality
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2, CURRENT_YEAR


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


def test_job_1(spark):
    # Define the schema using namedtuple
    NBARecord = namedtuple(
        "NBARecord",
        [
            "game_id",
            "team_id",
            "team_abbreviation",
            "team_city",
            "player_id",
            "player_name",
            "nickname",
            "start_position",
            "comment",
            "min",
            "fgm",
        ],
    )

    # Create the input data using namedtuple
    input_data = [
        NBARecord(1, 10, "TA", "CityA", 100, "PlayerA", "NickA", "G", "Good", "30", 5),
        NBARecord(1, 10, "TA", "CityA", 100, "PlayerA", "NickA", "G", "Good", "30", 5),
        NBARecord(2, 20, "TB", "CityB", 200, "PlayerB", "NickB", "F", "Bad", "20", 3),
    ]
    input_df = spark.createDataFrame(input_data)

    # Register the DataFrame as a temporary view
    input_df.createOrReplaceTempView("nba_game_details")

    # Define the expected schema using namedtuple
    ExpectedNBARecord = namedtuple(
        "ExpectedNBARecord",
        [
            "game_id",
            "team_id",
            "team_abbreviation",
            "team_city",
            "player_id",
            "player_name",
            "nickname",
            "start_position",
            "comment",
            "min",
            "fgm",
        ],
    )

    # Create the expected output data using namedtuple
    expected_data = [
        ExpectedNBARecord(
            1, 10, "TA", "CityA", 100, "PlayerA", "NickA", "G", "Good", "30", 5
        ),
        ExpectedNBARecord(
            2, 20, "TB", "CityB", 200, "PlayerB", "NickB", "F", "Bad", "20", 3
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    # Run the job_1 function
    output_df = job_1(spark, "nba_game_details")

    # Compare the output DataFrame with the expected DataFrame
    assert_df_equality(output_df, expected_df, ignore_row_order=True)


# Unit Test for job_2

def test_job_2(spark):
    # Create sample data for source_table
    source_data = [
        ("Actor A", 1, "Film A1", 101, 100, 8.5, CURRENT_YEAR + 1),
        ("Actor B", 2, "Film B1", 102, 150, 7.5, CURRENT_YEAR + 1),
        ("Actor C", 3, "Film C1", 103, 200, 6.5, CURRENT_YEAR + 1),
        ("Actor D", 4, "Film D1", 104, 50, 5.5, CURRENT_YEAR + 1),
    ]
    source_schema = "actor STRING, actor_id INT, film STRING, film_id INT, votes INT, rating DOUBLE, year INT"
    source_df = spark.createDataFrame(source_data, schema=source_schema)
    source_df.createOrReplaceTempView("actor_films")

    # Create sample data for cumulative_table
    cumulative_data = [
        ("Actor A", 1, [("Film A0", 100, 100, 9.0, CURRENT_YEAR)], CURRENT_YEAR),
        ("Actor B", 2, [("Film B0", 101, 150, 8.0, CURRENT_YEAR)], CURRENT_YEAR),
        ("Actor C", 3, [("Film C0", 102, 200, 7.0, CURRENT_YEAR)], CURRENT_YEAR),
        ("Actor D", 4, [("Film D0", 103, 50, 6.0, CURRENT_YEAR)], CURRENT_YEAR),
    ]
    cumulative_schema = "actor STRING, actor_id INT, films ARRAY<STRUCT<film: STRING, film_id: INT, votes: INT, rating: DOUBLE, year: INT>>, CURRENT_YEAR INT"
    cumulative_df = spark.createDataFrame(cumulative_data, schema=cumulative_schema)
    cumulative_df.createOrReplaceTempView("actors")

    # Expected output
    expected_data = [
        ("Actor A", 1, [("Film A1", 101, 100, 8.5, CURRENT_YEAR + 1), ("Film A0", 100, 100, 9.0, CURRENT_YEAR)], "star", True, CURRENT_YEAR + 1),
        ("Actor B", 2, [("Film B1", 102, 150, 7.5, CURRENT_YEAR + 1), ("Film B0", 101, 150, 8.0, CURRENT_YEAR)], "good", True, CURRENT_YEAR + 1),
        ("Actor C", 3, [("Film C1", 103, 200, 6.5, CURRENT_YEAR + 1), ("Film C0", 102, 200, 7.0, CURRENT_YEAR)], "average", True, CURRENT_YEAR + 1),
        ("Actor D", 4, [("Film D1", 104, 50, 5.5, CURRENT_YEAR + 1), ("Film D0", 103, 50, 6.0, CURRENT_YEAR)], "bad", True, CURRENT_YEAR + 1),
    ]
    expected_schema = "actor STRING, actor_id INT, films ARRAY<STRUCT<film: STRING, film_id: INT, votes: INT, rating: DOUBLE, year: INT>>, quality_class STRING, is_active BOOLEAN, CURRENT_YEAR INT"
    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Run the job
    result_df = job_2(spark, "actor_films", "actors", CURRENT_YEAR)

    # Compare the result with the expected output
    assert_df_equality(result_df, expected_df, ignore_nullable=True)