from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date, datetime
import pytz

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2


def test_job_1(spark_session):
    """
    Test the job_1 function.

    This function tests the job_1 function by creating input data and existing data,
    creating DataFrames from them, and then calling the job_1 function with the input DataFrame.
    It then compares the actual output DataFrame with the expected output DataFrame.

    Parameters:
        spark_session (SparkSession): The SparkSession object used to create DataFrames.

    Returns:
        None
    """

    # Define the named tuples for input data and expected data. 
    # ActorsFilms follows the schema needed for actor_films
    # Actors follows the schema needed for actors
    # Films defines the struct structure for films field in Actors 
    ActorsFilms = namedtuple("ActorsFilms", "actor actor_id year rating film film_id votes") 
    Actors = namedtuple("Actors", "actor actor_id films quality_class is_active current_year")
    Films = namedtuple("Films", "year film votes rating film_id")

    # Define the input data for testing
    input_data = [
        ActorsFilms(
            actor='Robert Downey Jr.',
            actor_id = 'nm0000375',
            year = 2014,
            rating = 8.0,
            film = 'Chef Test 1',
            film_id = 'tt2883512',
            votes = 10
        ),
        ActorsFilms(
            actor='Robert Downey Jr.',
            actor_id = 'nm0000375',
            year = 2014,
            rating = 9.0,
            film = 'The Judge Test 2',
            film_id = 'tt1872194',
            votes = 100
        )
    ]

    # Define the existing data for testing. 
    # Existing data refers to records already in output table as query reads from previous year (existing data) and current year (input data)
    existing_data = [
        Actors(
            actor = 'Robert Downey Jr.',
            actor_id = 'nm0000375',
            films = [
                Films(
                    year = 2013,
                    film = 'Iron Man 3',
                    votes = 300,
                    rating = 7.1,
                    film_id = 'tt1300854'
                )
            ],
            quality_class = 'star',
            is_active = True,
            current_year = 2013
        )
    ]

    # create expected data for testing
    expected_data = [
        Actors(
            actor = 'Robert Downey Jr.',
            actor_id = 'nm0000375',
            films = [
                Films(
                    year = 2014,
                    film = 'Chef Test 1',
                    votes = 10,
                    rating = 8.0,
                    film_id = 'tt2883512'
                ),
                Films(
                    year = 2014,
                    film = 'The Judge Test 2',
                    votes = 100,
                    rating = 9.0,
                    film_id = 'tt1872194'
                ),
                Films(
                    year = 2013,
                    film = 'Iron Man 3',
                    votes = 300,
                    rating = 7.1,
                    film_id = 'tt1300854'
                )
            ],
            quality_class = 'star',
            is_active = True,
            current_year = 2014
        )
    ]

    # Create DataFrames from the input and existing data and views the query will reference
    input_df = spark_session.createDataFrame(input_data)
    input_df.createOrReplaceTempView("actor_films")

    existing_df = spark_session.createDataFrame(existing_data)
    existing_df.createOrReplaceTempView("actors")


    # Call the job_1 function with the output tablename and compare the actual output DataFrame with the expected output DataFrame
    actual_df = job_1(spark_session, "actors")
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job_2(spark_session):
    """
    Test the job_2 function.

    This function tests the job_1 function by creating input data and existing data,
    creating DataFrames from them, and then calling the job_1 function with the input DataFrame.
    It then compares the actual output DataFrame with the expected output DataFrame using the
    assert_df_equality function.

    Parameters:
        spark_session (SparkSession): The SparkSession object used to create DataFrames.

    Returns:
        None
    """

    # Define the named tuples for input data and expected data.
    # WebEvents follows the schema needed for web_events
    # Devices follows the schema needed for devices
    # UserDevices defines the schema for the output table and existing data (user_cummulative_devices)
    WebEvents = namedtuple("WebEvents", "user_id device_id referrer	host url event_time") 
    Devices = namedtuple("Devices", "browser_type device_id")
    UserDevices = namedtuple("UserDevices", "user_id browser_type dates_active date")

    # Define the input data for testing. Input define for web_events and devices
    input_events_data = [
        WebEvents(-9999999999, 12345678, 'www.google.com', 'www.eczachly.com', '/', datetime(2023, 1, 7, 10, 30, tzinfo=pytz.UTC))
    ]

    input_devices_data = [
        Devices('Google', 12345678)
    ]

    # Define the existing data for testing
    # Existing data refers to records already in output table (user_cummulative_devices) as query reads from previous year (existing data) and current year
    existing_data = [
        UserDevices(
            user_id = -9999999999,
            browser_type = 'Google',
            dates_active = [datetime.strptime("2023-01-06", "%Y-%m-%d").date()],
            date = datetime.strptime("2023-01-06", "%Y-%m-%d").date()
        )
    ]

    # create expected data for testing
    expected_data = [
        UserDevices(
            user_id = -9999999999,
            browser_type = 'Google',
            dates_active = [datetime.strptime("2023-01-07", "%Y-%m-%d").date(), datetime.strptime("2023-01-06", "%Y-%m-%d").date()],
            date = datetime.strptime("2023-01-07", "%Y-%m-%d").date()
        )
    ]

    # Create DataFrames from the input and existing data and views the query will reference
    input_events_df = spark_session.createDataFrame(input_events_data)
    input_events_df.createOrReplaceTempView("web_events")

    input_devices_df = spark_session.createDataFrame(input_devices_data)
    input_devices_df.createOrReplaceTempView("devices")

    existing_df = spark_session.createDataFrame(existing_data)
    existing_df.createOrReplaceTempView("user_devices_cummulated")

    # Call the job_2 function with the output tablename and compare the actual output DataFrame with the expected output DataFrame
    actual_df = job_2(spark_session, "user_devices_cummulated")
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)