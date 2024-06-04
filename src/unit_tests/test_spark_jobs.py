from chispa.dataframe_comparer import assert_df_equality

from chispa.dataframe_comparer import *

from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType,BooleanType, DateType
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

from datetime import date

# from jobs import job_2

# define named tuples
Actorfilms = namedtuple("Actorfilms", "actor actor_id film year votes rating film_id")
ActorfilmsAgg = namedtuple("ActorfilmsAgg", "actor actor_id quality_class current_year")

#namedtuples for job2
nba_players = namedtuple("nba_players", "player_name height college country	draft_year draft_round draft_number	is_active years_since_last_active current_season")
nba_players_streaked = namedtuple("nba_players_streaked", "player_name is_active start_season end_season current_season")

#test job_1
def test_job_1(spark_session):
    input_table_name: str = "actor_films"
    source_data = [
        Actorfilms("Milton Berle", "nm0000926", "Rebecca of Sunnybrook Farm", 1917, 782, 6.1, "tt0008499"),
        Actorfilms("Zasu Pitts", "nm0686032", "Rebecca of Sunnybrook Farm", 1917, 782, 6.1, "tt0008499"),
        Actorfilms("Zasu Pitts", "nm0686032", "A Modern Musketeer", 1917, 264, 6.8, "tt0008309")
    ]
    source_df = spark_session.createDataFrame(source_data)

    
    actual_df = job_1(spark_session, source_df, input_table_name)
    expected_data = [
        ActorfilmsAgg("Milton Berle", "nm0000926", "average", 1917),
        ActorfilmsAgg("Zasu Pitts", "nm0686032", "average", 1917)
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df.sort("actor"), expected_df.sort("actor"))

# test job_2
def test_job_2(spark_session):
    input_table_name: str = "nba_players"
    source_data1 = [
        nba_players("Michael Jordan", "6-6", "North Carolina", "USA", 1984, 1, 3, False, 3, 2000)
    ]
    source_df1 = spark_session.createDataFrame(source_data1)

    
    actual_df = job_2(spark_session, source_df1, input_table_name)
    expected_data = [
        nba_players_streaked("Michael Jordan", '0', 2000, 2000, 2001)
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df,ignore_nullable=True)
