from chispa.dataframe_comparer import *
from ..jobs.job_1 import current_year, job_1
from ..jobs.job_2 import month_start, job_2
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DoubleType, LongType
from collections import namedtuple

import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def test_actors_acc(spark_session):

    actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
    actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")
    film = namedtuple("films", "film votes rating film_id year")
    # Creating fake input for actor_films table
    input_data = [
        actor_films("Fred Astaire", "nm0000001", "Ghost Story", 1981, 7731, 6.3, "tt0082449"),
        actor_films("Fred Astaire", "nm0000001", "Shall We Dance", 1937, 6603, 7.5, "tt0029546"),
        actor_films("Paul Newman", "nm0000056", "Where the Money Is", current_year +1 ,
                     5747, 6.2, "tt0149367"),
        actor_films("John Cleese", "nm0000092", "Isn't She Great", current_year+1,
                     2281, 5.5, "tt0141399"),
        actor_films("John Cleese", "nm0000092", "The Magic Pudding", current_year+1,
                     428, 5.9, "tt0141399")
    ]

    # Generating the actor_films data based on the fake input
    input_data = spark_session.createDataFrame(input_data)
    input_data.createOrReplaceTempView("actor_films")

    # Expected output based on our input
    # Since this job starts with and empty table,
    # than it filters the year by the current year + 1,
    # it must return two rows, one for each actor that made films in the current year + 1 date
    expected_output = [
        actor("Paul Newman", "nm0000056", [film("Where the Money Is",
                                            5747,
                                            6.2,
                                            "tt0149367",
                                            current_year +1)],
               "average" , True , current_year + 1),
        actor("John Cleese", "nm0000092", [film("Isn't She Great",
                                            2281,
                                            5.5,
                                            "tt0141399",
                                            current_year +1),
                                           film("The Magic Pudding",
                                            428,
                                            5.9,
                                            "tt0141399",
                                            current_year +1)],
                                              "bad", True, current_year+1)
    ]

    expected_df = spark_session.createDataFrame(expected_output)

    initial_df = spark_session.createDataFrame(data=[], schema=StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), True),
            StructField("votes", LongType(), True),
            StructField("rating", DoubleType(), True),
            StructField("film_id", StringType(), True),
            StructField("year", LongType(), True)
        ]))),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", LongType(), True),
        ]))
    
    initial_df.createOrReplaceTempView("actors")

    # Running our actual job
    actual_df = job_1(spark_session, "actors", current_year)

    # Verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)


# def test_host_activity_reduced(spark_session):

#     daily_web_metrics = namedtuple("DailyWebMetrics", "actor actor_id film year votes rating film_id")
#     actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")

#     # Creating fake input for actor_films table
#     input_data = [
        
#     ]

#     # Generating the actor_films data based on the fake input
#     input_data = spark_session.createDataFrame(input_data)
#     input_data.createOrReplaceTempView("barrocaeric.daily_web_metrics")

#     # Expected output based on our input
#     # Since this job starts with and empty table,
#     expected_output = [
        
#     ]

#     expected_df = spark_session.createDataFrame(expected_output)

#     # Running our actual job
#     actual_df = job_2(spark_session, "barrocaeric.host_activity_reduced", month_start)

#     # Verifying that the dataframes are identical
#     assert_df_equality(actual_df, expected_df, ignore_nullable=True)