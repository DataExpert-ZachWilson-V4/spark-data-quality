from chispa.dataframe_comparer import *
from ..jobs.job_1 import current_year, job_1
from ..jobs.job_2 import month_start, job_2
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, BooleanType, DoubleType, LongType
from collections import namedtuple

# Was having trouble to test it locally, thus I added this to help pytest find pyspark engine
import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# This is not an extensive test, just a demonstration.
# It tests if films made by the same actor in the same year are grouped together in the films array
# and if films made by an actor in years different from the current one are excluded from the insertion
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

    # Needed to create the first iteration of the table since it did not exist in the environment
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
    # Ignoring row orders to make it easier to match the expected to the actual data
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)

# Demonstrative of an unit test for an reduced table
# running at the second day of the started month.
# It tests if the metrics counts are aggregated correclty in the metric_array column
# And if data is not repeated
def test_host_activity_reduced(spark_session):

    daily_web_metrics = namedtuple("DailyWebMetrics", "host metric_name metric_value date")
    host_activity_reduced = namedtuple("HostActivityReduced", "host metric_name metric_array month_start")
    current_date = '2023-08-02'

    # Creating fake input for daily_web_metrics table
    input_data = [
        daily_web_metrics("admin.zachwilson.tech", "visited_home_page", 5, "2022-08-08"),
        daily_web_metrics("www.zachwilson.tech", "visited_home_page", 53, month_start),
        daily_web_metrics("www.eczachly.com", "visited_home_page", 53, month_start),
        daily_web_metrics("dutchengineer.techcreator.io", "visited_home_page", 30, month_start),
        daily_web_metrics("dutchengineer.techcreator.io", "visited_home_page", 10, current_date)
    ]

    # Generating the daily_web_metrics data based on the fake input
    input_data = spark_session.createDataFrame(input_data)
    input_data.createOrReplaceTempView("daily_web_metrics")

     # Simulates the reduced table with the first day populated
    initial_data = [
        host_activity_reduced("www.zachwilson.tech", "visited_home_page", [53], month_start),
        host_activity_reduced("www.eczachly.com", "visited_home_page", [53], month_start),
        host_activity_reduced("dutchengineer.techcreator.io", "visited_home_page", [30], month_start)
    ]
   
    initial_df = spark_session.createDataFrame(initial_data)
    initial_df.createOrReplaceTempView("host_activity_reduced")

    # Expected output based on our input
    expected_output = [
        host_activity_reduced("www.eczachly.com", "visited_home_page", [53, None], month_start),
        host_activity_reduced("www.zachwilson.tech", "visited_home_page", [53, None], month_start),
        host_activity_reduced("dutchengineer.techcreator.io", "visited_home_page", [30, 10], month_start)
    ]

    expected_df = spark_session.createDataFrame(expected_output)

    # Running our actual job
    actual_df = job_2(spark_session, "host_activity_reduced", month_start, current_date)

    # Verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True, ignore_row_order=True)