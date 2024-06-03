from chispa.dataframe_comparer import *

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from collections import namedtuple

# define named tuples
Actors = namedtuple ("Actors", "actor actor_id films quality_class is_active current_year")
Actors_Films = namedtuple("Actors_Films", "actor actor_id film year votes rating film_id")

def test_job_1(spark):
    input_data = [
        Actors_Films("Robert Downey Jr.", "nm0000375", "Sherlock Holmes", 2009, 587083, 7.6, "tt0988045"),
        Actors_Films("Robert Downey Jr.", "nm0000375", "The Soloist", 2009, 51753, 6.7, "tt0821642"),
        Actors_Films("Brad Pitt", "nm0000093", "Inglourious Basterds", 2009, 1285359, 8.3, "tt0361748"),
        Actors_Films("Kevin Bacon", "nm0000102", "My One and Only", 2009, 8538, 6.5, "tt1185431")
    ]

    # expected output based on our input
    expected_output = [
        Actors(
            actor = 'Robert Downey Jr.', 
            actor_id = 'nm0000375', 
            films = [
                {'year': 2009, 'film': "Sherlock Holmes", 'votes': 587083, 'rating': 7.6, 'film_id':"tt0988045"},
                {'year': 2009, 'film': "The Soloist", 'votes': 51753, 'rating': 6.7, 'film_id': "tt0821642"},
                {'year': 2008, 'film': "Iron Man", 'votes': 951095, 'rating': 7.9, 'film_id': "tt0371746"},
                {'year': 2008, 'film': "The Incredible Hulk", 'votes': 439469, 'rating': 6.7, 'film_id': "tt0800080"},
                {'year': 2008, 'film': "Tropic Thunder", 'votes': 381427, 'rating': 7, 'film_id': "tt0942385"}
            ],
            quality_class = 'good',
            is_active = 'true',
            current_year = 2009
        ),
        Actors(
            actor = 'Brad Pitt',
            actor_id = 'nm0000093',
            films = [
                {'year': 2009, 'film': "Inglourious Basterds", 'votes': 1285359, 'rating': 8.3, 'film_id':"tt0361748"},
                {'year': 2008, 'film': "The Curious Case of Benjamin Button", 'votes': 595444, 'rating': 7.8, 'film_id': "tt0421715"},
                {'year': 2008, 'film': "Burn After Reading", 'votes': 312495, 'rating': 7, 'film_id': "tt0887883"}
            ],
            quality_class = 'star',
            is_active = 'true',
            current_year = 2009
        ),
        Actors(
            actor = 'Kevin Bacon',
            actor_id = 'nm0000102',
            films = [
                {'year': 2009, 'film': "My One and Only", 'votes': 8538, 'rating': 6.5, 'film_id':"tt1185431"},
                {'year': 2008, 'film': "Frost/Nixon", 'votes': 103893, 'rating': 7.7, 'film_id': "tt0870111"}
            ],
            quality_class = 'good',
            is_active = 'true',
            current_year = 2009
        )
    ]

    input_data_df = spark.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("Actors_Films")
    
    expected_df = spark.createDataFrame(expected_output)
    
    # running the job
    actual_df = job_1(sparks, "Actors_Films")
    
    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

# define named tuples
web_events = namedtuple("web_events", "user_id device_id referrer host url event_time")
devices = namedtuple("devices", "device_id browser_type os_type device_type")
devices_cumulated = namedtuple("devices_cumulated", "user_id browser_type dates_active date")

def test_job_2(spark):
    input_data_web_events = [
        web_events(-2077270748, -2012543895, NONE, "www.zachwilson.tech", "/", "2023-01-02 00:15:32.122 UTC"),
        web_events(-2077270748, -2012543895, NONE, "www.zachwilson.tech", "/robots.txt", "2023-01-02 08:52:10.788 UTC"),
        web_events(-2077270748, -2012543895, "", "www.zachwilson.tech", "/robots.txt", "2023-01-02 12:06:37.575 UTC"),
        web_events(-188443929, 27181149, "", "admin.zachwilson.tech", "/", "2023-01-02 02:49:45.934 UTC"),
        web_events(-2052853069, -290659081, "", "www.zachwilson.tech", "/", "2023-01-02 03:50:18.059 UTC"),
        web_events(-2052853069, -290659081, "", "www.zachwilson.tech", "/", "2023-01-02 17:12:42.304 UTC"),
        web_events(-2052853069, -290659081, "", "www.zachwilson.tech", "/", "2023-01-02 17:16:31.808 UTC"),
        web_events(-2052853069, -290659081, "", "www.zachwilson.tech", "/atom.xml", "2023-01-02 23:33:37.920 UTC")
    ]

    input_data_devices = [
        devices(-2012543895, 'Googlebot', 'Other', 'Spider'),
        devices(27181149, 'Chrome', 'Windows', 'Other'),
        devices(-290659081, 'bingbot', 'Other', 'Spider')
    ]

    input_data_cumulated = [
        devices_cumulated(-2077270748, 'Googlebot', [date(2023, 1, 1)], date(2023, 1, 1)),
        devices_cumulated(-2052853069, 'bingbot', [date(2023, 1, 1)], date(2023, 1, 1))
    ]

    # expected output based on our input
    expected_output = [
        devices_cumulated(user_id = -2077270748, browser_type = 'Googlebot', dates_active = [date(2023, 1, 1), date(2023, 1, 2)], date = date(2023, 1, 2)),
        devices_cumulated(user_id = -188443929, browser_type = 'Chrome', dates_active = [date(2023, 1, 2)], date = date(2023, 1, 2)),
        devices_cumulated(user_id = -2052853069, browser_type = 'bingbot', dates_active = [date(2023, 1, 1), date(2023, 1, 2)], date = date(2023, 1, 2))
    ]

    web_events_df = spark.createDataFrame(input_data_web_events)
    web_events_df.createOrReplaceTempView("web_events")

    devices_df = spark.createDataFrame(input_data_devices)
    devices_df.createOrReplaceTempView("devices")

    cumulated_df = spark.createDataFrame(input_data_cumulated)
    cumulated_df.createOrReplaceTempView("devices_cumulated")

    expected_df = spark.createDataFrame(expected_output)

    # running the job
    actual_df = job_2(spark, "devices_cumulated")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
