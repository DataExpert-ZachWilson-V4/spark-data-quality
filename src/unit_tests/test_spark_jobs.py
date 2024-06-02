from chispa.dataframe_comparer import *
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from collections import namedtuple

job1_input = namedtuple("job1_input", "actor	actor_id	films	quality_class	is_active	current_year")
job1_output = namedtuple("job1_output", "actor_id	actor	quality_class	is_active	start_date	end_date	current_year")
job2_input = namedtuple("job2_input", "game_id	team_id	team_abbreviation	team_city	player_id	player_name	nickname	start_position	comment	min	fgm	fga	fg_pct	fg3m	fg3a	fg3_pct	ftm	fta	ft_pct	oreb	dreb	reb	ast	stl	blk	to	pf	pts	plus_minus")
job2_output = namedtuple("job2_output", "game_id team_id player_id row_count")

def test_job1(spark):
    input_data = [
        job1_input(
          actor="Famous Actor", 
          actor_id="nm9999999", 
          films=[["Phobias","tt8129682",289,3.7]],
          quality_class="bad",
          is_active=True, 
          current_year=2018),
        job1_input(
          actor="Famous Actor", 
          actor_id="nm9999999", 
          films=[["Phobias","tt8129682",289,3.7],["SampleMovie","tt8123921",1764,4.7]],
          quality_class="bad",
          is_active=True, 
          current_year=2021)
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = job1(spark, input_dataframe)
    expected_output = [
        job1_output(
            actor_id="nm9999999",
            actor="Famous Actor",
            quality_class="bad",
            is_active=True,
            start_date=2018,
            end_date=2021,
            current_year=2021
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


def test_job2(spark):
    input_data = [
        job2_input(
          game_id=60700155,
          team_id=1610612742,
          team_abbreviation="DAL",
          team_city="Dallas",
          player_id=436,
        player_name="Junan Howard",
            nickname="lorem ipsum",
            start_position=0,
            comment="lorem ipsum",	
            min="lorem ipsum",
            fgm=0,
            fga=0,
            fg_pct=0,
            fg3m=0,
            fg3a=0,
            fg3_pct=0,
            ftm=0,
            fta=0,
            ft_pct=0,
            oreb=0,	
            dreb=0,
            reb=0,	
            ast=0,
            stl=0,
            blk=0,
            to=0,
            pf=0,
            pts=0,
            plus_minus=0
            ),
        job2_input(
          game_id=60700155,
          team_id=1610612742,
          team_abbreviation="DAL",
          team_city="Dallas",
          player_id=436,
        player_name="Junan Howard",
            nickname="lorem ipsum",
            start_position=0,
            comment="lorem ipsum",	
            min="lorem ipsum",
            fgm=0,
            fga=0,
            fg_pct=0,
            fg3m=0,
            fg3a=0,
            fg3_pct=0,
            ftm=0,
            fta=0,
            ft_pct=0,
            oreb=0,	
            dreb=0,
            reb=0,	
            ast=0,
            stl=0,
            blk=0,
            to=0,
            pf=0,
            pts=0,
            plus_minus=0
            )
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = job2(spark, input_dataframe)
    expected_output = [
        job2_output(
            game_id=60700155,
            team_id=1610612742,
            player_id=436,
            row_count=1
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
