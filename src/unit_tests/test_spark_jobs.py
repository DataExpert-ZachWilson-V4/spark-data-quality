from chispa.dataframe_comparer import assert_df_equality

from chispa.dataframe_comparer import *

from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType,BooleanType, DateType

from jobs import job_2

# define named tuples
PlayerSeason = namedtuple("PlayerSeason", "player_name is_active current_season")
PlayerScd = namedtuple("PlayerScd", "player_name is_active start_season end_season")


def test_job_1(spark_session):
    input_table_name: str = "nba_players"
    source_data = [
        PlayerSeason("Michael Jordan", True, 2011),
        PlayerSeason("Michael Jordan", True, 2012),
        PlayerSeason("Michael Jordan", True, 2013),
        PlayerSeason("Michael Jordan", False, 2014),
        PlayerSeason("Michael Jordan", False, 2015),
        PlayerSeason("LeBron James", True, 2013),
        PlayerSeason("LeBron James", True, 2014),
        PlayerSeason("LeBron James", True, 2015),
        PlayerSeason("Scottie Pippen", True, 2011),
        PlayerSeason("Scottie Pippen", False, 2012),
        PlayerSeason("Scottie Pippen", False, 2013),
        PlayerSeason("Scottie Pippen", True, 2014),
        PlayerSeason("Scottie Pippen", True, 2015)
    ]
    source_df = spark_session.createDataFrame(source_data)

    from jobs.job_1 import job_1
    actual_df = job_1(spark_session, source_df, input_table_name)
    expected_data = [
        PlayerScd("Michael Jordan", True, 2011, 2013),
        PlayerScd("Michael Jordan", False, 2014, 2015),
        PlayerScd("Scottie Pippen", True, 2011, 2011),
        PlayerScd("Scottie Pippen", False, 2012, 2013),
        PlayerScd("Scottie Pippen", True, 2014, 2015),
        PlayerScd("LeBron James", True, 2013, 2015)
    ]
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df.sort("player_name", "start_season"), expected_df.sort("player_name", "start_season"))





def test_job_2(spark_session):

    # define named tuples
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")

    input_data = [
        actors(actor="Liam Neeson", actor_id="nm0000553", films=[[2021,"The Marksman",2333,5.9,"tt6902332"]], quality_class=1, is_active=1, current_year='2021'),
        actors(actor="Luenell", actor_id="nm0132685", films=[[2021,"Coming 2 America",49700,5.3,"tt6802400"]], quality_class=0, is_active=1, current_year='2021'),
        actors(actor="Ruta Lee", actor_id="nm0498181", films=[[2021,"Senior Moment",208,5.2,"tt6588950"]], quality_class=1, is_active=0, current_year='2021'),
        actors(actor="Michael McElhatton", actor_id="nm0568385", films=[[2021,"Zack Snyder's Justice League",242474,8.2,"tt12361974"]], quality_class=1, is_active=1, current_year='2021')
    ]

    input_df = spark_session.createDataFrame(input_data)

    # create a temp table from input_df
    input_df.createOrReplaceTempView("actors")
    test_output_table_name = "actors"

    actual_df = job_2.job_2(spark_session, test_output_table_name)

    print(f"Resulting Schema {actual_df.schema}")

    expected_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("quality_class", LongType(), True),
        StructField("is_active", LongType(), True),
        StructField("start_date", StringType(), True),
        StructField("end_date", StringType(), True),
        StructField("current_year", StringType(), True)
    ])

    assert actual_df.schema == expected_schema