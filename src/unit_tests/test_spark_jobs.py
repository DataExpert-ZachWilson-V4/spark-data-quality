# test_spark_jobs.py
import pytest
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, ArrayType, StructType,
    BooleanType, DoubleType, LongType,
    TimestampType, DateType
)
from pyspark.sql import Row
from pyspark.sql.functions import to_timestamp

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2


# Test using AAA (Arrange-Act-Assert) methodoly 
def test_job_1_function(spark_session):
    # ======================================================
    # Arrange (Prepara context for the test and expected results)
    spark = spark_session
    # Define the schema based on the provided DDL
    actor_films_schema = StructType([
        StructField("actor", StringType()),
        StructField("actor_id", StringType()),
        StructField("film", StringType()),
        StructField("year", IntegerType()),
        StructField("votes", IntegerType()),
        StructField("rating", DoubleType()),
        StructField("film_id", StringType()),
    ])
    
    actor_films_data = [
        ("Charles Chaplin", "nm0000122", "Tillie's Punctured Romance", 1914, 3301, 6.3, "tt0004707"),
        ("Lionel Barrymore", "nm0000859", "Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"),
        ("Milton Berle", "nm0000926", "The Perils of Pauline", 1914, 942, 6.3, "tt0004465"),
        ("Lillian Gish", "nm0001273", "Judith of Bethulia", 1914, 1259, 6.1, "tt0004181"),
        ("Lillian Gish", "nm0001273", "Home, Sweet Home", 1914, 190, 5.8, "tt0003167"),
        ("Harold Lloyd", "nm0516001", "The Patchwork Girl of Oz", 1914, 398, 5.5, "tt0004457")
    ]
    df = spark.createDataFrame(actor_films_data, schema=actor_films_schema)
    df.createOrReplaceTempView("actor_films")

    actors_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("films", ArrayType(
            StructType([
                StructField("year", IntegerType(), True),
                StructField("film", StringType(), True),
                StructField("votes", LongType(), True),
                StructField("rating", DoubleType(), True),
                StructField("film_id", StringType(), True)
            ])
        ), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", IntegerType(), True)
    ])
    # Define the output table name
    output_table_name = "actors"

    df_actors = spark.createDataFrame([], schema=actors_schema)
    df_actors.createOrReplaceTempView(output_table_name)

    data = [
        ("Milton Berle", "nm0000926", [("1914", "The Perils of Pauline", 942, 6.3, "tt0004465")], "average", True, 1914),
        ("Lionel Barrymore", "nm0000859", [("1914", "Judith of Bethulia", 1259, 6.1, "tt0004181")], "average", True, 1914),
        ("Charles Chaplin", "nm0000122", [("1914", "Tillie's Punctured Romance", 3301, 6.3, "tt0004707")], "average", True, 1914),
        ("Harold Lloyd", "nm0516001", [("1914", "The Patchwork Girl of Oz", 398, 5.5, "tt0004457")], "bad", True, 1914),
        ("Lillian Gish", "nm0001273", [("1914", "Judith of Bethulia", 1259, 6.1, "tt0004181"), ("1914", "Home, Sweet Home", 190, 5.8, "tt0003167")], "bad", True, 1914),
    ]

    # Convert data to use Row object for nested structure
    nested_data = [Row(actor=x[0], actor_id=x[1], films=[Row(year=int(y[0]), film=y[1], votes=int(y[2]), rating=float(y[3]), film_id=y[4]) for y in x[2]], quality_class=x[3], is_active=x[4], current_year=x[5]) for x in data]

    # Create DataFrame
    expected_df = spark.createDataFrame(nested_data, actors_schema)

    # =============================================
    # Act
    # Run the job_1 function
    result_df = job_1(spark, output_table_name)

    # ===============================================    
    # Assert
    # Is the result what we would expect ?
    assert result_df.collect() == expected_df.collect(), "The output does not match the expected results"


# Test using AAA (Arrange-Act-Assert) methodoly
# Test the empty case for the main JOIN in the query 2 statement 
def test_job_2_function(spark_session):
    # =================================
    # Arrange
    spark = spark_session
    # Schema for web_events
    web_events_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("device_id", IntegerType(), True),
        StructField("referrer", StringType(), True),
        StructField("host", StringType(), True),
        StructField("url", StringType(), True),
        StructField("event_time", StringType(), True),
    ])

    # Schema for devices
    devices_schema = StructType([
        StructField("device_id", IntegerType(), True),
        StructField("browser_type", StringType(), True),
        StructField("os_type", StringType(), True),
        StructField("device_type", StringType(), True),
    ])
    # Data for web_events
    web_events_data = [
        (1967566579, -1138341683, None, "www.eczachly.com", "/", "2021-01-18 23:57:37.316"),
        (1272828233, -643696601, None, "www.zachwilson.tech", "/", "2021-01-18 00:10:52.986"),
        (694175222, 1847648591, None, "www.eczachly.com", "/", "2021-01-18 00:15:29.251"),
    ]

    # Data for devices
    devices_data = [
        (-2147042689, "Firefox", "Ubuntu", "Other"),
        (-2146219609, "WhatsApp", "Other", "Spider"),
        (-2145574618, "Chrome Mobile", "Android", "Generic Smartphone"),
        (-2144707350, "Chrome Mobile WebView", "Android", "Samsung SM-G988B"),
        (-2143813999, "Mobile Safari UI/WKWebView", "iOS", "iPhone"),
    ]

    # Creating DataFrames
    web_events_df = spark.createDataFrame(web_events_data, schema=web_events_schema)
    web_events_df = web_events_df.withColumn("event_time", to_timestamp(web_events_df.event_time, 'yyyy-MM-dd HH:mm:ss.SSS'))
    devices_df = spark.createDataFrame(devices_data, schema=devices_schema)
    
    web_events_df.createOrReplaceTempView("web_events")
    devices_df.createOrReplaceTempView("devices")
    
    expected_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("browser_type", StringType(), True),
        StructField("dates_active", ArrayType(DateType()), True),
        StructField("current_date", DateType(), True)
    ])
    
    output_table_name = "user_devices_cumulated"
    expected_df = spark.createDataFrame([], expected_schema)
    expected_df.createOrReplaceTempView(output_table_name)
    # =============================================
    # Act
    # Run the job_2 function
    result_df = job_2(spark, output_table_name)

    # ===============================================    
    # Assert
    # Is the result what we would expect ?
    assert result_df.collect() == expected_df.collect(), "The output does not match the expected results"

