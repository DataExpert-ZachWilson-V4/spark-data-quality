from chispa.dataframe_comparer import *

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from collections import namedtuple

Actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")
ActorFilm = namedtuple("ActorFilm", "actor actor_id year film votes rating film_id")
Film = namedtuple("Film", "year film votes rating film_id")

def test_job_1(spark_session):
    actor_films_input = [
        ActorFilm("William Bassett", "A123456", 2001, "Wow I love Spark", 1500, 9.0, 'F001'), 
        ActorFilm("William Bassett", "A123456", 2001, 'So very much!', 2000, 9.0, 'F002'), 
        ActorFilm("William Bassett", "A123456", 2004, "This should not show up :D", 2000, 9.0, 'F002'), 
        ActorFilm("Billiam Bassett", "A123457", 2001, "...so very little...", 3000, 9.0, 'F003')
    ]

    actors_input = [
        Actor("Jacquelyn Bassett", "A123458", [Film(2000, "What is going on here", 2000, 9.0, 'F004')], "good", True, 2000),
        Actor("Billiam Bassett", "A123457", [Film(2000, "Wow I love Spark", 1500, 9.0, 'F001')], "good", True, 2000)
    ]

    actor_films_df = spark_session.createDataFrame(actor_films_input)
    actor_films_df.createOrReplaceTempView("actor_films")
    actors_df = spark_session.createDataFrame(actors_input)
    actors_df.createOrReplaceTempView("actors")

    expected_output = [
        Actor("William Bassett", "A123456", [Film(2001, "Wow I love Spark", 1500, 9.0, 'F001'), Film(2001, 'So very much!', 2000, 9.0, 'F002')], "star", True, 2001),
        Actor("Billiam Bassett", "A123457", [Film(2001, "...so very little...", 3000, 9.0, 'F003'), Film(2000, "Wow I love Spark", 1500, 9.0, 'F001')], "star", True, 2001),
        Actor("Jacquelyn Bassett", "A123458", [Film(2000, "What is going on here", 2000, 9.0, 'F004')], "bad", False, 2001)
    ]

    expected_output_df = spark_session.createDataFrame(expected_output)

    actual_df = job_1(spark_session, "actor_films", "actors")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)


def test_job_2(spark_session):
    NbaGameDetail = namedtuple(
    "NbaGameDetail",
    "game_id team_id player_id team_abbreviation team_city player_name nickname min start_position fgm fga"
    )

    input_data = [
        NbaGameDetail(1, 2, 3, "WPB", "Macungie", "William Bassett", "willypb", 234, "I don't play sports", 3, 3),
        NbaGameDetail(1, 2, 3, "WPB", "Macungie", "William Bassett", "willypb", 45, "I don't play sports", 2, 2),
        NbaGameDetail(1, 2, 3, "WPB", "Macungie", "William Bassett", "willypb", 56, "I don't play sports", 4, 4),
        NbaGameDetail(1, 3, 4, "WKB", "Mahwah", "Bill Bassett", "bxcarwilly", 30, "F", 0, 3),
        NbaGameDetail(1, 3, 4, "WKB", "Mahwah", "Bill Bassett", "bxcarwilly", 47, "F", 0, 6),
    ]

    game_details_df = spark_session.createDataFrame(input_data)
    game_details_df.createOrReplaceTempView("game_details")

    expected_output = [
        NbaGameDetail(1, 2, 3, "WPB", "Macungie", "William Bassett", "willypb", 234, "I don't play sports", 3, 3),
        NbaGameDetail(1, 3, 4, "WKB", "Mahwah", "Bill Bassett", "bxcarwilly", 30, "F", 0, 3)
    ]

    expected_output_df = spark_session.createDataFrame(expected_output)

    actual_df = job_2(spark_session, "game_details")

    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)