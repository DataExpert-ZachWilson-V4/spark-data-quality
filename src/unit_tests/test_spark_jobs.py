from chispa.dataframe_comparer import *
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from collections import namedtuple
from datetime import date
from conftest import *
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, StringType, FloatType, DateType, ArrayType
from pyspark.sql.functions import col, lit, to_date, array, coalesce, expr
# from pyspark.sql import SQLContext


# define named tuples
actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
actor_history_scd = namedtuple("actor_history_scd", "actor_id	quality_class	is_active	start_date	end_date	current_date")

def test_job_1(spark_session):
    input_data = [
        actors("Milton Berle","nm0000926",[[1920,"The Mark of Zorro",2247,7.1,"tt0011439"]],"good","true", 1920),
        actors("Boris Karloff","nm0000472",[[1920,"The Last of the Mohicans",1142,6.7,"tt0011387"]],"average","true", 1920),
        actors("Noah Beery Jr.","nm0000890",[[1920,"The Mark of Zorro",2247,7.1,"tt0011439"]],"good","true", 1920)
    ]

    # expected output based on our input
    expected_output = [
        actor_history_scd("nm0000926", "good", "true", 1920, 1920, 2024),
        actor_history_scd("nm0000472", "average", "true", 1920, 1920, 2024),
        actor_history_scd("nm0000890", "good", "true", 1920, 1920, 2024)
    ]

    schema = StructType([
        StructField("actor_id", StringType(), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", StringType(), True),
        StructField("start_date", LongType(), True),
        StructField("end_date", LongType(), True),
        StructField("current_date", IntegerType(), True),
    ])

    input_data_df = spark_session.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("actors")

    # spark_session.sql("SELECT * FROM actors").show()

    expected_output_df = spark_session.createDataFrame(expected_output, schema)

    empty_output_df = spark_session.createDataFrame([],schema)
    empty_output_df.createOrReplaceTempView("actors_history_scd")

    # running the job
    actual_df = job_1(spark_session,"actors","actors_history_scd")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)


# define named tuples
hosts_cumulated = namedtuple("hosts_cumulated", "host	host_activity_datelist	date")
web_event = namedtuple("web_events", "user_id	device_id	referrer	host	url	event_time")

def test_job_2(spark_session):
    web_input_data = [
        web_event(1931897548,532630305,"null","www.eczachly.com",	"/", "2023-01-07 10:53:57.021 UTC"),
        web_event(1931897548,532630305,"null","www.eczachly.com",	"/contact", "2023-01-07 10:54:08.572 UTC"),
        web_event(1931897548,532630305,"null","www.eczachly.com",	"/about", "2023-01-07 10:54:09.676 UTC"),
        web_event(1500498728,-1217993711,"null","www.zachwilson.tech","/","2023-01-07 03:22:04.781 UTC"),
        web_event(-2129181599,-1516965605,"null","admin.zachwilson.tech","/","2023-01-07 00:57:31.988 UTC")
    ]

    # expected output based on our input
    expected_output = [
         hosts_cumulated("admin.zachwilson.tech", [date(2023, 1, 7)], date(2023, 1, 7)),
        hosts_cumulated("www.zachwilson.tech", [date(2023, 1, 7)], date(2023, 1, 7)),
        hosts_cumulated("www.eczachly.com", [date(2023, 1, 7)], date(2023, 1, 7))
    ]

    web_event_df = spark_session.createDataFrame(web_input_data)
    web_event_df.createOrReplaceTempView("web_event")

    schema = StructType([
        StructField("host", StringType(), True),
        StructField("host_activity_datelist", ArrayType(DateType()), True),
        StructField("date", DateType(), True)
    ])

    empty_output_df = spark_session.createDataFrame([], schema)
    empty_output_df.createOrReplaceTempView("hosts_cumulated")

    expected_output_df = spark_session.createDataFrame(expected_output,schema)
    expected_output_df.show()
    # running the job
    actual_df = job_2(spark_session,"web_event", "hosts_cumulated")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)