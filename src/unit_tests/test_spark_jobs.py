from pyspark.sql.types import *
from collections import namedtuple
from datetime import datetime
from chispa.dataframe_comparer import  assert_df_equality
from ..jobs import job_1
from ..jobs import job_2
import pytest
from pyspark.sql import SparkSession


def dedup_date(spark_session):
    fct_nba_game_details_cols = ["game_id", "team_id", "player_id", "dim_team_abbreviation", "dim_player_name", "dim_start_position", "dim_did_not_dress", "dim_not_with_team", "m_seconds_played", "m_field_goals_made", "m_field_goals_attempted", "m_3_pointers_made", "m_3_pointers_attempted", "m_free_throws_made", "m_free_throws_attempted", "m_offensive_rebounds", "m_defensive_rebounds", "m_rebounds", "m_assists", "m_steals", "m_blocks", "m_turnovers", "m_personal_fouls", "m_points", "m_plus_minus", "dim_game_date", "dim_season", "dim_team_did_win"]
    fct_nba_game_details = namedtuple("fct_nba_game_details", fct_nba_game_details_cols)
    fct_nba_game_details_dedup_schema = StructType([StructField("game_id", LongType(), True), StructField("team_id", LongType(), True), StructField("player_id", LongType(), True), StructField("dim_team_abbreviation", StringType(), True), StructField("dim_player_name", StringType(), True), StructField("dim_start_position", StringType(), True), StructField("dim_did_not_dress", BooleanType(), True), StructField("dim_not_with_team", BooleanType(), True), StructField("m_seconds_played", IntegerType(), True), StructField("m_field_goals_made", DoubleType(), True), StructField("m_field_goals_attempted", DoubleType(), True), StructField("m_3_pointers_made", DoubleType(), True), StructField("m_3_pointers_attempted", DoubleType(), True), StructField("m_free_throws_made", DoubleType(), True), StructField("m_free_throws_attempted", DoubleType(), True), StructField("m_offensive_rebounds", DoubleType(), True), StructField("m_defensive_rebounds", DoubleType(), True), StructField("m_rebounds", DoubleType(), True), StructField("m_assists", DoubleType(), True), StructField("m_steals", DoubleType(), True), StructField("m_blocks", DoubleType(), True), StructField("m_turnovers", DoubleType(), True), StructField("m_personal_fouls", DoubleType(), True), StructField("m_points", DoubleType(), True), StructField("m_plus_minus", DoubleType(), True), StructField("dim_game_date", DateType(), True), StructField("dim_season", IntegerType(), True), StructField("dim_team_did_win", BooleanType(), True)])
    actual_fct_nba_game_details = [
    fct_nba_game_details(game_id=1, team_id=100, player_id=1000, dim_team_abbreviation="LAL", dim_player_name="LeBron James", dim_start_position="SF", dim_did_not_dress=False, dim_not_with_team=False, m_seconds_played=2400, m_field_goals_made=10, m_field_goals_attempted=20, m_3_pointers_made=2, m_3_pointers_attempted=5, m_free_throws_made=5, m_free_throws_attempted=6, m_offensive_rebounds=1, m_defensive_rebounds=7, m_rebounds=8, m_assists=9, m_steals=2, m_blocks=1, m_turnovers=3, m_personal_fouls=2, m_points=27, m_plus_minus=15, dim_game_date="2023-01-01", dim_season=2023, dim_team_did_win=True),
    fct_nba_game_details(game_id=2, team_id=101, player_id=1001, dim_team_abbreviation="GSW", dim_player_name="Stephen Curry", dim_start_position="PG", dim_did_not_dress=False, dim_not_with_team=False, m_seconds_played=2400, m_field_goals_made=12, m_field_goals_attempted=22, m_3_pointers_made=5, m_3_pointers_attempted=10, m_free_throws_made=4, m_free_throws_attempted=4, m_offensive_rebounds=0, m_defensive_rebounds=5, m_rebounds=5, m_assists=7, m_steals=1, m_blocks=0, m_turnovers=4, m_personal_fouls=3, m_points=33, m_plus_minus=10, dim_game_date="2023-01-01", dim_season=2023, dim_team_did_win=False)
    ]
    expected_nba_game_details_data = [
    fct_nba_game_details(game_id=1, team_id=100, player_id=1000, dim_team_abbreviation="LAL", dim_player_name="LeBron James", dim_start_position="SF", dim_did_not_dress=False, dim_not_with_team=False, m_seconds_played=2400, m_field_goals_made=10, m_field_goals_attempted=20, m_3_pointers_made=2, m_3_pointers_attempted=5, m_free_throws_made=5, m_free_throws_attempted=6, m_offensive_rebounds=1, m_defensive_rebounds=7, m_rebounds=8, m_assists=9, m_steals=2, m_blocks=1, m_turnovers=3, m_personal_fouls=2, m_points=27, m_plus_minus=15, dim_game_date=datetime.strptime("2023-01-01", "%Y-%m-%d"), dim_season=2023, dim_team_did_win=True),
    fct_nba_game_details(game_id=2, team_id=101, player_id=1001, dim_team_abbreviation="GSW", dim_player_name="Stephen Curry", dim_start_position="PG", dim_did_not_dress=False, dim_not_with_team=False, m_seconds_played=2400, m_field_goals_made=12, m_field_goals_attempted=22, m_3_pointers_made=5, m_3_pointers_attempted=10, m_free_throws_made=4, m_free_throws_attempted=4, m_offensive_rebounds=0, m_defensive_rebounds=5, m_rebounds=5, m_assists=7, m_steals=1, m_blocks=0, m_turnovers=4, m_personal_fouls=3, m_points=33, m_plus_minus=10, dim_game_date=datetime.strptime("2023-01-01", "%Y-%m-%d"), dim_season=2023, dim_team_did_win=False)
    ]
    spark = SparkSession.builder.master("local").appName("chispa_test").getOrCreate()
    fct_nba_game_details_df = spark.createDataFrame(actual_fct_nba_game_details,schema=fct_nba_game_details_dedup_schema)
    fct_nba_game_details_df.createOrReplaceTempView("fct_nba_game_details")
    expected_df = spark.createDataFrame(expected_nba_game_details_data, schema=fct_nba_game_details_dedup_schema)

    actual_df = job_1.job_1(spark,"fct_nba_game_details")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)

def test_inc_host_data(spark_session):
    host_cols = ["host", "host_activity_datelist", "date"]
    Hosts = namedtuple("Hosts", host_cols)
    web_event_cols = ["host", "event_time"]
    WebEvents = namedtuple("WebEvents", web_event_cols)

    host_schema = StructType([StructField("host", StringType()), StructField("host_activity_datelist", ArrayType(DateType())), StructField("date", DateType())])
    actual_host_data = []
    actual_web_event_data = [
    WebEvents(host="www.example.com", event_time="2023-01-01 21:29:03.519"),
    WebEvents(host="www.example.com", event_time="2023-01-01 00:05:39.907"),
    WebEvents(host="admin.zachwilson.io.tech", event_time="2023-01-01 00:01:39.907"),
    WebEvents(host="admin.zachwilson.testing.tech", event_time="2023-01-01 00:08:13.852")
    ]
    expected_data = [
    Hosts(host="www.example.com", host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d")], date=datetime.strptime("2023-01-01", "%Y-%m-%d")),
    Hosts(host="admin.zachwilson.io.tech", host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d")], date=datetime.strptime("2023-01-01", "%Y-%m-%d")),
    Hosts(host="admin.zachwilson.testing.tech", host_activity_datelist=[datetime.strptime("2023-01-01", "%Y-%m-%d")], date=datetime.strptime("2023-01-01", "%Y-%m-%d"))
    ]
    spark = SparkSession.builder.master("local").appName("chispa_test").getOrCreate()
    host_df = spark.createDataFrame(actual_host_data, schema=host_schema)
    web_events_df = spark.createDataFrame(actual_web_event_data)
    expected_df = spark.createDataFrame(expected_data, schema=host_schema)

    host_df.createOrReplaceTempView("host_cumulated")
    web_events_df.createOrReplaceTempView("web_events")

    actual_df = job_2.job_2(spark,"host_cumulated","web_events","2023-01-01")
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)
