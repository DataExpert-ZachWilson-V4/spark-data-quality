from chispa.dataframe_comparer import *

from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from collections import namedtuple

Actors = namedtuple ("Actors", "actor actor_id films quality_class is_active current_year")
Actors_Films = namedtuple("Actors_Films", "actor actor_id film year votes rating film_id")
film = namedtuple("Films", "year film votes rating film_id")

def test_job_1(spark):
    input_data = [
        Actors_Films("Robert Downey Jr.", "nm0000375", "Sherlock Holmes", 2009, 587083, 7.6, "tt0988045"),
        Actors_Films("Robert Downey Jr.", "nm0000375", "The Soloist", 2009, 51753, 6.7, "tt0821642"),
        Actors_Films("Brad Pitt", "nm0000093", "Inglourious Basterds", 2009, 1285359, 8.3, "tt0361748"),
        Actors_Films("Kevin Bacon", "nm0000102", "My One and Only", 2009, 8538, 6.5, "tt1185431")
    ]

    expected_output = [
        Actors('Robert Downey Jr.', 
                'nm0000375', 
                [Films(2009, "Sherlock Holmes", 587083, 7.6, "tt0988045"),
                 Films(2009, "The Soloist", 51753, 6.7, "tt0821642"),
                 Films(2008, "Iron Man", 951095, 7.9, "tt0371746"),
                 Films(2008, "The Incredible Hulk", 439469, 6.7, "tt0800080"),
                 Films(2008, "Tropic Thunder", 381427, 7, "tt0942385")
                 ]
                'good',
                'true',
                2009
        ),
        Actors('Brad Pitt',
                'nm0000093',
                [Films(2009, "Inglourious Basterds", 1285359, 8.3, "tt0361748"),
                 Films(2008, "he Curious Case of Benjamin Button", 595444, 7.8, "tt0421715"),
                 Films(2008, "Burn After Reading", 312495, 7, "tt0887883")
                ],
                'star',
                'true',
                2009
        ),
        Actors('Kevin Bacon',
                'nm0000102',
                [Films(2009, "My One and Only", 8538, 6.5, "tt1185431"),
                 Films(2009, "Frost/Nixon", 103893, 7.7, "tt0870111")
                ],
                'good',
                'true',
                2009
        )
    ]

    input_data_df = spark.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("Actors_Films")
    
    expected_df = spark.createDataFrame(expected_output)
    
    actual_df = job_1(sparks, "Actors_Films")
    
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


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
        web_events(-2052853069, -290659081, "", "www.zachwilson.tech", "/atom.xml", "2023-01-02 23:33:37.920 UTC")
    ]

    input_data_devices = [
        devices(-2012543895, 'Googlebot', 'Other', 'Spider'),
        devices(27181149, 'Chrome', 'Windows', 'Other'),
        devices(-290659081, 'bingbot', 'Other', 'Spider')
    ]

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

    actual_df = job_2(spark, "devices_cumulated")

    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
