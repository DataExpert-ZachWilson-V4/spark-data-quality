import pytest

from jobs.job_1 import job_1
from jobs.job_2 import job_2
from chispa.dataframe_comparer import *
from collections import namedtuple

job1_in = namedtuple("job1_in", "actor actor_id	films quality_class	is_active current_year")
job1_out = namedtuple("job1_out", "actor_id	actor quality_class	is_active start_date end_date current_year")

job2_in = namedtuple("job2_in", " game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus")
job2_out = namedtuple("job2_out", "game_id team_id player_id dup")


def test_job1(spark):
    input_data = [
        job1_in(
          actor="ABC DEF", 
          actor_id="abc123", 
          films=[["example","ex123",123,4.5]],
          quality_class="good",
          is_active=True, 
          current_year=2000)
    ]

    input_dataframe = spark.createDataFrame(input_data)
    in_df = job_1(spark, input_dataframe)

    expected_output = [
        job1_out(
            actor_id="abc123",
            actor="ABC DEF",
            quality_class="bad",
            is_active=True,
            start_date=2000,
            end_date=2003,
            current_year=2010
        )
    ]
    out_df = spark.createDataFrame(expected_output)
    
    assert_df_equality(in_df, out_df, ignore_nullable=True)