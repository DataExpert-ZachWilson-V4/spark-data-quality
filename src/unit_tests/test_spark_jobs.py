from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from datetime import date, datetime
import pytz

from src.jobs.Update_Actors_CT import Update_Actors_CT
from src.jobs.Update_User_Devices_Cumulated import Update_User_Devices_Cumulated

def test_job_1(spark_session):

    # Define named tuples for input data and expected data
    actor_films = namedtuple("actor_film", "actor actor_id film year votes rating film_id") 
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    films = namedtuple("films", "film votes rating film_id") 

    # Input data for testing
    input_actor_films_data = [
        actor_films(
            actor='Lillian Gish',
            actor_id = 'nm0001273',
            film = 'The Birth of a Nation',
            year = 1915,
            votes = 22989,
            rating = 6.3,
            film_id = 'tt0004972'
        )
    ]

    # Existing data for testing
    existing_actors_data = [
        actors(
            actor = 'Lillian Gish',
            actor_id = 'nm0001273',
            films = [
                films(
                    film = 'Judith of Bethulia',
                    votes = 1259,
                    rating = 6.1,
                    film_id = 'tt0004181'
                ),
                films(
                    film = 'Home, Sweet Home',
                    votes = 190,
                    rating = 5.8,
                    film_id = 'tt0003167'
                )
            ],
            quality_class = 'bad',
            is_active = True,
            current_year = 1914
        )
    ]

    # Expected data for testing
    expected_data = [
        actors(
            actor = 'Lillian Gish',
            actor_id = 'nm0001273',
            films = [
                films(
                    film = 'Judith of Bethulia',
                    votes = 1259,
                    rating = 6.1,
                    film_id = 'tt0004181'
                ),
                films(
                    film = 'Home, Sweet Home',
                    votes = 190,
                    rating = 5.8,
                    film_id = 'tt0003167'
                ),
                films(
                    film = 'The Birth of a Nation',
                    votes = 22989,
                    rating = 6.3,
                    film_id = 'tt0004972'
                )

            ],
            quality_class = 'average',
            is_active = True,
            current_year = 1915
        )
    ]

    # DataFrame of the input and existing data and views the test will reference
    input_df = spark_session.createDataFrame(input_actor_films_data)
    input_df.createOrReplaceTempView("actor_films")

    existing_df = spark_session.createDataFrame(existing_actors_data)
    existing_df.createOrReplaceTempView("actors")

    # Run the Update_Actors_CT function and compare the output with expected
    actual_df = Update_Actors_CT(spark_session, "actors", 1915)
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job_2(spark_session):

    # Define named tuples for input data and expected data
    web_events = namedtuple("web_events", "user_id device_id referrer host url event_time")
    devices = namedtuple("devices", "device_id browser_type os_type device_type")
    user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

    # Input data for testing
    input_events_data = [
        web_events(
            user_id = 495022226,
            device_id = -2012543895, 
            referrer = NULL, 
            host = 'www.zachwilson.tech', 
            url = '/', 
            event_time = datetime(2023, 1, 3, 0, 31, tzinfo=pytz.UTC)
        )
    ]

    input_devices_data = [
        devices(
            device_id = -2012543895, 
            browser_type = 'Googlebot', 
            os_type = 'Other', 
            device_type = 'Spider'
        )
    ]

    # Existing data for testing
    existing_user_devices_cumulated_data = [
        user_devices_cumulated(
            user_id = 495022226,
            browser_type = 'Googlebot',
            dates_active = [datetime.strptime("2023-01-02", "%Y-%m-%d").date()],
            date = datetime.strptime("2023-01-02", "%Y-%m-%d").date()
        )
    ]

    # Expected data for testing
    expected_data = [
        user_devices_cumulated(
            user_id = 495022226,
            browser_type = 'Googlebot',
            dates_active = [datetime.strptime("2023-01-03", "%Y-%m-%d").date(), datetime.strptime("2023-01-02", "%Y-%m-%d").date()],
            date = datetime.strptime("2023-01-03", "%Y-%m-%d").date()
        )
    ]

    # DataFrame of the input and existing data and views the test will reference
    input_events_df = spark_session.createDataFrame(input_events_data)
    input_events_df.createOrReplaceTempView("web_events")

    input_devices_df = spark_session.createDataFrame(input_devices_data)
    input_devices_df.createOrReplaceTempView("devices")

    existing_df = spark_session.createDataFrame(existing_user_devices_cumulated_data)
    existing_df.createOrReplaceTempView("user_devices_cumulated")

    # Run the Update_User_Devices_Cumulated function and compare the output with expected
    actual_df = Update_User_Devices_Cumulated(spark_session, "user_devices_cumulated")
    expected_df = spark_session.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
