from chispa.dataframe_comparer import *
from jobs.job_2 import job_2
from collections import namedtuple


NbaGame = namedtuple(
    "NbaGame",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus",
)

NbaGameDedupped = namedtuple(
    "NbaGameDedupped",
    "row_number game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus",
)

input_nbagame_data = [
    NbaGame(
        game_id=20900108,
        team_id=1610612758,
        team_abbreviation="SAC",
        team_city="Sacramento",
        player_id=201150,
        player_name="Spencer Hawes",
        nickname="None",
        start_position="C",
        comment="None",
        min="31:59",
        fgm=5,
        fga=10,
        fg_pct=0.5,
        fg3m=0,
        fg3a=3,
        fg3_pct=0.6,
        ftm=2,
        fta=2,
        ft_pct=1,
        oreb=2,
        dreb=6,
        reb=8,
        ast=2,
        stl=0,
        blk=4,
        to=1,
        pf=4,
        pts=12,
        plus_minus=-5,
    ),
    NbaGame(
        game_id=21600529,
        team_id=1610612766,
        team_abbreviation="CHA",
        team_city="Charlotte",
        player_id=201939,
        player_name="Stephen Curry",
        nickname="Steph",
        start_position="PG",
        comment="None",
        min="36:42",
        fgm=14,
        fga=26,
        fg_pct=0.5,
        fg3m=7,
        fg3a=15,
        fg3_pct=0.4,
        ftm=6,
        fta=6,
        ft_pct=1,
        oreb=0,
        dreb=6,
        reb=6,
        ast=9,
        stl=1,
        blk=0,
        to=2,
        pf=3,
        pts=41,
        plus_minus=12,
    ),
]


# Define the schema for the films array
def test_spark_queries_2(spark_session):

    input_nbagame_dataframe = spark_session.createDataFrame(input_nbagame_data)
    actual_dataframe = job_2(spark_session, input_nbagame_dataframe)
    

    expected_output = [
        NbaGameDedupped(
            row_number=1,
            game_id=20900108,
            team_id=1610612758,
            team_abbreviation="SAC",
            team_city="Sacramento",
            player_id=201150,
            player_name="Spencer Hawes",
            nickname="None",
            start_position="C",
            comment="None",
            min="31:59",
            fgm=5,
            fga=10,
            fg_pct=0.5,
            fg3m=0,
            fg3a=3,
            fg3_pct=0.6,
            ftm=2,
            fta=2,
            ft_pct=1,
            oreb=2,
            dreb=6,
            reb=8,
            ast=2,
            stl=0,
            blk=4,
            to=1,
            pf=4,
            pts=12,
            plus_minus=-5,
        ),
        NbaGameDedupped(
            row_number=1,
            game_id=21600529,
            team_id=1610612766,
            team_abbreviation="CHA",
            team_city="Charlotte",
            player_id=201939,
            player_name="Stephen Curry",
            nickname="Steph",
            start_position="PG",
            comment="None",
            min="36:42",
            fgm=14,
            fga=26,
            fg_pct=0.5,
            fg3m=7,
            fg3a=15,
            fg3_pct=0.4,
            ftm=6,
            fta=6,
            ft_pct=1,
            oreb=0,
            dreb=6,
            reb=6,
            ast=9,
            stl=1,
            blk=0,
            to=2,
            pf=3,
            pts=41,
            plus_minus=12,
        ),
    ]

    expected_df = spark_session.createDataFrame(expected_output)
    assert_df_equality(actual_dataframe, expected_df, ignore_nullable=True)
