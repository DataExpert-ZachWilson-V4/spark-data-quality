from chispa.dataframe_comparer import *

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from collections import namedtuple

Players = namedtuple("Players", "game_id team_id player_id player_name")
Actors = namedtuple("Actors", "actor actor_id quality_class is_active start_date end_date current_year")

def test_job_1_generation(spark):
    input_data = [
        Players(21400714, 1610612746, 201150, "Spencer Hawes"),
        Players(21400714, 1610612746, 201150, "Spencer Hawes1")
    ]

    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("nba_game_details")
    
    actual_df = job_1(spark, output_table_name="nba_game_details")
    expected_output = [
        Players(
            game_id=21400714,
            team_id=1610612746,
            player_id=201150,
            player_name="Spencer Hawes"
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
    
def test_job_2_generation(spark):
    input_data = [
        Actors("A", 1, "Good", True, 2005, 2005, 2005)
    ]


    input_dataframe = spark.createDataFrame(input_data)
    input_dataframe.createOrReplaceTempView("actors")
    
    actual_df = job_2(spark, output_table_name="actors")
    expected_output = [
        Actors(
            actor='A', 
            actor_id=1,
            quality_class='Good', 
            is_active=True,
            start_date=2005, 
            end_date=2005,
            current_year=2005
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)
