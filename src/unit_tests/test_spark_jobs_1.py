from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from collections import namedtuple

Actor = namedtuple(
    "Actor", "actor actor_id films average_rating quality_class is_active current_year"
)

ActorFilms = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")

NbaGame = namedtuple(
    "NbaGame",
    "game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus",
)

NbaGameDedupped = namedtuple(
    "NbaGameDedupped",
    "row_number game_id team_id team_abbreviation team_city player_id player_name nickname start_position comment min fgm fga fg_pct fg3m fg3a fg3_pct ftm fta ft_pct oreb dreb reb ast stl blk to pf pts plus_minus",
)

input_actor_films_data = [
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="Jayne Mansfield's Car",
        year=2012,
        votes=2945,
        rating=6.3,
        film_id="tt1781840",
    ),
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="X-Men: First Class",
        year=2011,
        votes=650154,
        rating=7.7,
        film_id="tt1270798",
    ),
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="Crazy, Stupid, Love.",
        year=2011,
        votes=478740,
        rating=7.4,
        film_id="tt1570728",
    ),
    ActorFilms(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        film="Elephant White",
        year=2011,
        votes=10447,
        rating=5.1,
        film_id="tt1578882",
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="John Dies at the End",
        year=2012,
        votes=36363,
        rating=6.4,
        film_id="tt1783732",
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="At Any Price",
        year=2012,
        votes=7282,
        rating=5.6,
        film_id="tt1937449",
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="Hellbenders",
        year=2012,
        votes=1401,
        rating=4.8,
        film_id="tt1865393",
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="Green Lantern",
        year=2011,
        votes=268721,
        rating=5.5,
        film_id="tt1133985",
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        film="Cowboys & Aliens",
        year=2011,
        votes=208961,
        rating=6.0,
        film_id="tt0409847",
    ),
]


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


expected_output = [
    Actor(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        films=[
            {
                "film": "Jayne Mansfield's Car",
                "year": 2012,
                "votes": 2945,
                "rating": 6.3,
                "film_id": "tt1781840",
            },
            {
                "film": "X-Men: First Class",
                "year": 2011,
                "votes": 650154,
                "rating": 7.7,
                "film_id": "tt1270798",
            },
            {
                "film": "Crazy, Stupid, Love.",
                "year": 2011,
                "votes": 478740,
                "rating": 7.4,
                "film_id": "tt1570728",
            },
            {
                "film": "Elephant White",
                "year": 2011,
                "votes": 10447,
                "rating": 5.1,
                "film_id": "tt1578882",
            },
        ],
        average_rating=6.3,
        quality_class="average",
        is_active=True,
        current_year=2012,
    ),
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        films=[
            {
                "film": "John Dies at the End",
                "year": 2012,
                "votes": 36363,
                "rating": 6.4,
                "film_id": "tt1783732",
            },
            {
                "film": "At Any Price",
                "year": 2012,
                "votes": 7282,
                "rating": 5.6,
                "film_id": "tt1937449",
            },
            {
                "film": "Hellbenders",
                "year": 2012,
                "votes": 1401,
                "rating": 4.8,
                "film_id": "tt1865393",
            },
            {
                "film": "Green Lantern",
                "year": 2011,
                "votes": 268721,
                "rating": 5.5,
                "film_id": "tt1133985",
            },
            {
                "film": "Cowboys & Aliens",
                "year": 2011,
                "votes": 208961,
                "rating": 6.0,
                "film_id": "tt0409847",
            },
        ],
        average_rating=5.60,
        quality_class="bad",
        is_active=True,
        current_year=2012,
    ),
]


# Define the schema for the films array
def test_spark_queries_1(spark_session):

    input_actor_films_dataframe = spark_session.createDataFrame(input_actor_films_data)
    input_films_dataframe = spark_session.createDataFrame(expected_output)

    actual_dataframe = job_1(
        spark_session, input_actor_films_dataframe, input_films_dataframe
    )

    expected_df = spark_session.createDataFrame(expected_output)
    assert_df_equality(actual_dataframe, expected_df, ignore_nullable=True)
