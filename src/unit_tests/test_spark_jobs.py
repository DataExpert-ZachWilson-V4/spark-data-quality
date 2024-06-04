from datetime import datetime
from chispa.dataframe_comparer import assert_df_equality
import pytz
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, ArrayType, IntegerType, DateType, TimestampType
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
def test_job1(spark_session):
    output_table_name: str = "actors"
   # Create DataFrame for actors table schema, so that data types are correct
    actors_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("films", ArrayType(
            StructType([
                StructField("year", IntegerType(), True),
                StructField("film", StringType(), True),
                StructField("votes", IntegerType(), True),
                StructField("rating", DoubleType(), True),
                StructField("film_id", StringType(), True)
            ])
        ), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), False),
        StructField("current_year", IntegerType(), True)
    ])
    yesterday_actors_data = [
        Row(
            actor = 'Actor A',
            actor_id = 'actorid1',
            films = [
                Row(
                    year = 1914,
                    film = 'actorAfilm1',
                    votes = 1000,
                    rating = 6.1,
                    film_id = 'filmid1'
                ),
                Row(
                    year = 1914,
                    film = 'actorAfilm2',
                    votes = 200,
                    rating = 5.8,
                    film_id = 'filmid2'
                )
            ],
            quality_class = 'bad',
            is_active = True,
            current_year = 1914
        )
    ]
    yesterday_df = spark_session.createDataFrame(yesterday_actors_data, actors_schema)
    yesterday_df.createOrReplaceTempView(output_table_name)
    actor_films_schema = StructType([
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("film", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("votes", IntegerType(), True),
        StructField("rating", DoubleType(), True),
        StructField("film_id", StringType(), True),
    ])
    actor_films_data = [
        Row(
            actor='Actor A',
            actor_id = 'actorid1',
            film = 'actorAfilm3',
            year = 1915,
            votes = 300,
            rating = 6.3,
            film_id = 'filmid3'
        ),
        Row(
            actor='Actor B',
            actor_id = 'actorid2',
            film = 'actorBfilm1',
            year = 1915,
            votes = 500,
            rating = 8.0,
            film_id = 'filmid4'
        )
    ]
    actor_films_df = spark_session.createDataFrame(actor_films_data, actor_films_schema)
    actor_films_df.createOrReplaceTempView('actor_films')
    expected_data = [
        Row(
            actor = 'Actor A',
            actor_id = 'actorid1',
            films = [
                Row(
                    year = 1915,
                    film = 'actorAfilm3',
                    votes = 300,
                    rating = 6.3,
                    film_id = 'filmid3'
                ),
                Row(
                    year = 1914,
                    film = 'actorAfilm1',
                    votes = 1000,
                    rating = 6.1,
                    film_id = 'filmid1'
                ),
                Row(
                    year = 1914,
                    film = 'actorAfilm2',
                    votes = 200,
                    rating = 5.8,
                    film_id = 'filmid2'
                )
            ],
            quality_class = 'average',
            is_active = True,
            current_year = 1915
        ),
        Row(
            actor = 'Actor B',
            actor_id = 'actorid2',
            films = [
                Row(
                    year = 1915,
                    film = 'actorBfilm1',
                    votes = 500,
                    rating = 8.0,
                    film_id = 'filmid4'
                )
            ],
            quality_class = 'good',
            is_active = True,
            current_year = 1915
        )
    ]
    expected_df = spark_session.createDataFrame(expected_data, actors_schema)
    actual_df = job_1(spark_session, output_table_name)
    assert_df_equality(actual_df, expected_df)
def test_job2(spark_session):
    output_table_name: str = "hosts_cumulated"
    hosts_cumulated_schema = StructType([
        StructField("host", StringType(), True),
        StructField("host_activity_datelist", ArrayType(DateType()), True),
        StructField("date", DateType(), True)
    ])
    yesterday_hosts_cumulated_data = [
        Row(
            host = 'host1.com',
            host_activity_datelist = [datetime.strptime('2022-12-31', "%Y-%m-%d").date()],
            date = datetime.strptime('2022-12-31', "%Y-%m-%d").date()
        )
    ]
    yesterday_df = spark_session.createDataFrame(yesterday_hosts_cumulated_data,hosts_cumulated_schema)
    yesterday_df.createOrReplaceTempView(output_table_name)
    utc_timezone = pytz.utc
    web_events_schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("device_id", IntegerType(), False),
        StructField("referrer", StringType(), True),
        StructField("host", StringType(), False),
        StructField("url", StringType(), False),
        StructField("event_time", TimestampType(), False)
    ])
    web_events_data = [
        Row(
            user_id = 123,
            device_id = 567,
            referrer = None,
            host = 'host1.com',
            url = '/',
            event_time = datetime(2023, 1, 1, 10, 30, 0, 0, tzinfo=utc_timezone)
        ),
        Row(
            user_id = 123,
            device_id = 567,
            referrer = None,
            host = 'abcd.com',
            url = '/',
            event_time = datetime(2023, 1, 1, 10, 30, 0, 0, tzinfo=utc_timezone)
        )
    ]
    web_events_df = spark_session.createDataFrame(web_events_data,web_events_schema)
    web_events_df.createOrReplaceTempView('web_events')
    expected_data = [
        Row(
            host = 'abcd.com',
            host_activity_datelist = [datetime.strptime('2023-01-01', "%Y-%m-%d").date()],
            date = datetime.strptime('2023-01-01', "%Y-%m-%d").date()
        ),
        Row(
            host = 'host1.com',
            host_activity_datelist = [datetime.strptime('2023-01-01', "%Y-%m-%d").date(), datetime.strptime('2022-12-31', "%Y-%m-%d").date()],
            date = datetime.strptime('2023-01-01', "%Y-%m-%d").date()
        )
    ]
    expected_df = spark_session.createDataFrame(expected_data,hosts_cumulated_schema)
    actual_df = job_2(spark_session, output_table_name)
    assert_df_equality(actual_df, expected_df)