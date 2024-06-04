import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

# Test job 1
@pytest.fixture
def web_users_cumulated_data():
    return [
        Row(user_id=1, dates_active=[Row(date='2022-12-31')], date='2022-12-31'),
        Row(user_id=2, dates_active=[Row(date='2022-12-31')], date='2022-12-31'),
    ]

@pytest.fixture
def web_events_data():
    return [
        Row(user_id=1, event_time='2023-01-01 10:00:00'),
        Row(user_id=3, event_time='2023-01-01 11:00:00'),
    ]

@pytest.fixture
def web_users_cumulated_df(spark_session, web_users_cumulated_data):
    return spark_session.createDataFrame(web_users_cumulated_data)

@pytest.fixture
def web_events_df(spark_session, web_events_data):
    return spark_session.createDataFrame(web_events_data)


def test_job_1(spark_session, web_users_cumulated_df, web_events_df):
    web_users_cumulated_df.createOrReplaceTempView("dennisgera.web_users_cumulated")
    web_events_df.createOrReplaceTempView("bootcamp.web_events")
    
    result_df = job_1(spark_session, "dennisgera.web_users_cumulated", "2023-01-01")

    expected_data = [
        Row(user_id=1, dates_active=[Row(date='2023-01-01'), Row(date='2022-12-31')], date='2023-01-01'),
        Row(user_id=2, dates_active=[Row(date='2022-12-31')], date='2023-01-01'),
        Row(user_id=3, dates_active=[Row(date='2023-01-01')], date='2023-01-01'),
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    
    assert result_df.collect() == expected_df.collect()


# Test job_2
@pytest.fixture
def nba_game_details_data():
    return [
        Row(player_name="Player1", team_id="TeamA", reb=5, ast=7, blk=2),
        Row(player_name="Player1", team_id="TeamA", reb=3, ast=5, blk=1),
        Row(player_name="Player2", team_id="TeamB", reb=10, ast=4, blk=3),
    ]

@pytest.fixture
def nba_game_details_df(spark_session, nba_game_details_data):
    return spark_session.createDataFrame(nba_game_details_data)


def test_job_2(spark_session, nba_game_details_df):
    nba_game_details_df.createOrReplaceTempView("bootcamp.nba_game_details")
    
    result_df = job_2(spark_session, "fct_nba_game_details")

    expected_data = [
        Row(player_name="Player1", team_id="TeamA", number_of_games=2, total_rebounds=8, total_assists=12, total_blocks=3),
        Row(player_name="Player2", team_id="TeamB", number_of_games=1, total_rebounds=10, total_assists=4, total_blocks=3),
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    
    assert result_df.collect() == expected_df.collect()
