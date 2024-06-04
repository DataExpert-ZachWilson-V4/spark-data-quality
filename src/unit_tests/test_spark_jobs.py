from collections import namedtuple
from chispa.dataframe_comparer import *

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2


def test_deduped_table(spark):
    nba_game_details = namedtuple("nba_game_details",
                                  "game_id team_id team_abbreviation team_city player_id player_name")
    input_data = [
        nba_game_details(21600241, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes"),
        nba_game_details(21600241, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes"),
        nba_game_details(21600241, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes"),
        nba_game_details(21501144, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes"),
        nba_game_details(21501144, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes")
    ]
    input_df = spark.createDataFrame(input_data)
    input_df.createOrReplaceTempView("nba_game_details")

    actual_df = job_1(spark, "nba_game_details")
    expected_output = [
        nba_game_details(21600241, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes"),
        nba_game_details(21501144, 1610612766, "CHA", "Charlotte", 201150, "Spencer Hawes")
    ]
    expected_df = spark.createDataFrame(data=expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)


def test_cum_table(spark):
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id ")

    input_actor_films = [
        actor_films("John Ford", "nm0000406", "The Birth of a Nation", 1915, 22989, 6.3, "tt0004972"),
        actor_films("Lillian Gish", "nm0001273", "The Birth of a Nation", 1915, 22989, 6.3, "tt0004972")
    ]

    input_actors = [
        actors("Milton Berle", "nm0000926", [["The Perils of Pauline", 1914, 942, 6.3, "tt0004465"]], "average", True,
               1914),
        actors("Lionel Barrymore", "nm0000859", [["Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"]], "average", True,
               1914),
        actors("Lillian Gish", "nm0001273", [["Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"],
                                             ["Home, Sweet Home", 1914, 190, 5.8, "tt0003167"]], "bad", True, 1914)
    ]

    input_actor_films_df = spark.createDataFrame(input_actor_films)
    input_actor_films_df.createOrReplaceTempView("actor_films")

    input_actors_df = spark.createDataFrame(input_actors)
    input_actors_df.createOrReplaceTempView("actors")

    expected_output = [
        actors("Milton Berle", "nm0000926", [["The Perils of Pauline", 1914, 942, 6.3, "tt0004465"]], "average", True,
               1914),
        actors("Milton Berle", "nm0000926", [["The Perils of Pauline", 1914, 942, 6.3, "tt0004465"]], "average", False,
               1915),
        actors("Lionel Barrymore", "nm0000859", [["Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"]], "average", True,
               1914),
        actors("Lionel Barrymore", "nm0000859", [["Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"]], "average",
               False,
               1915),
        actors("Lillian Gish", "nm0001273", [["Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"],
                                             ["Home, Sweet Home", 1914, 190, 5.8, "tt0003167"]], "bad", True, 1914),
        actors("Lillian Gish", "nm0001273", [["The Birth of a Nation", 1915, 22989, 6.3, "tt0004972"],
                                             ["Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"],
                                             ["Home, Sweet Home", 1914, 190, 5.8, "tt0003167"]], "average", True, 1915),
        actors("John Ford", "nm0000406", [["The Birth of a Nation", 1915, 22989, 6.3, "tt0004972"]], "average", True,
               1915)
    ]

    expected_df = spark.createDataFrame(expected_output)
    actual_df = job_2(spark, "actors")

    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)
