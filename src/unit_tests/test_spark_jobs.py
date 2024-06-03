from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple

Game = namedtuple("Game","game_id team_id team_abbreviation team_city player_id player_name nickname start_position")

def test_game_dedupe(spark):
    input_data = [
        Game(11000076,1610612738,"BOS","Boston",201186,"Stephane Lasme",null,null),
        Game(11000076,1610612738,"BOS","Boston",201186,"Stephane Lasme",null,null)
    ]

    expected_output = [
        Game(11000076,1610612738,"BOS","Boston",201186,"Stephane Lasme",null,null)
    ]
    
    input_df = spark.createDataFrame(input_data)
    expected_output_df = spark.createDataFrame(expected_output)
    
    actual_df = job_1(spark, bootcamp.nba_game_details)

    assert_df_equality = (actual_df, expected_output_df, ignore_nullable=true)
