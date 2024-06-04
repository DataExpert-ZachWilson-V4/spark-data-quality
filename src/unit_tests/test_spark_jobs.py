from chispa.dataframe_comparer import assert_df_equality
import pytz
from pyspark.sql import Row
from jobs.job_2 import job_2, current_year
from jobs.job_1 import job_1
from collections import namedtuple
from datetime import date



#You want to setup first for testing job_1
ActorFilms = namedtuple(
    "ActorFilms","actor, actor_id, film, year, voes, rating, film_id"
)
Actors = namedtuple(
    "Actor", "actor, actor_id, films, quality_class, is_active, current_year"
)

#You want to setup next for testing job_2
Devices = namedtuple(
    "Devices", "device_id, browser_type, dates_active"
)
Web_Events = namedtuple(
    "Web_Events","user_id, device_id, referrer, host, url, event_time"
)
Devices_Cumulated = namedtuple(
    "Devices_Cumulated","user_id, brower_type, dates_active, date"
)

def test_job1(spark):
    current_year = 1950
    input_data_ActorFilms = [
        ActorFilms("Agnes Moorehead", "nm0001547", "Caged", 1950, 3558, 7.7, "tt0042296"
                   ),
        ActorFilms("Alan Hale Jr.", "nm0001308", "The West Point Story", 1950, 1176, 6.2, "tt0043123"
                  ),
        ActorFilms("Alan Ladd", "nm0000042", "Branded", 1950, 774, 6.7, "tt0042297"
                   )
    ]
    fake_ActorFilms_df = spark.createDataFrame(input_data_ActorFilms)
    fake_ActorFilms_df.createOrReplaceTempView("ActorFilms")

    input_data_Actors = [
        Actors(
            "Agnes Moorehead",
            "nm0001547",
            [
                Row(
                    year=current_year,
                    film="Captain Blackjack",
                    votes=111,
                    rating=5.9,
                    film_id="tt0042255"
                )
            ],
            "bad",
            True,
            current_year 
        ),   
    ]
    fake_Actors_df = spark.createDataFrame(input_data_Actors)
    fake_Actors_df.createOrReplaceTempView("Actors")

    #Expected Output
    expected_output = [
        Actors(
            "Agnes Moorehead",
            "nm0001547",
            [
                Row(
                    year=current_year,
                    film="Captain Blackjack",
                    votes=111,
                    rating=5.9,
                    film_id="tt0042255"
                ),
                Row(
                    year=current_year,
                    film="Caged",
                    votes=3558,
                    rating=7.7,
                    film_id="tt0042296"
                ),
            ],
            "average",
            True,
            current_year + 1,
        ),
        Actors(
            "Alan Hale Jr.",
            "nm0001308",
            [
                Row(
                    year=current_year + 1,
                    film="The West Point Story",
                    votes=1176,
                    rating=6.2,
                    film_id="tt0043123"
                )
            ],
            "average",
            True,
            current_year + 1
        ),    
        Actors(
            "Alan Ladd",
            "nm0000042",
            [
                Row(
                    year=current_year + 1,
                    film="Branded",
                    votes=774,
                    rating=6.7,
                    film_id="tt0042297"
                )
            ],
            "average",
            True,
            current_year + 1    
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    actual_df = job_1(spark, "Actors", current_year)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job2(spark):

    input_data_Devices = [
        Devices(-1894773659, "Chrome", [date(2022, 12, 31)]),
        Devices(1535782140, "Chrome", [date(2022, 12, 31)]),
        Devices(-2012543895, "Googlebot", [date(2023, 1, 1)]),
    ]
    fake_Devices_df = spark.createDataFrame(input_data_Devices)
    fake_Devices_df.createOrReplaceTempView("Devices")

    input_data_Web_Events = [
        Web_Events(
            -1463276726,
            1535782140,
            "http://zachwilson.tech",
            "www.zachwilson.tech",
            "/wp/wp-login.php",
            "2022-12-31 00:08:27.835 UTC",
        ),
        Web_Events(
            1272828233,
            -1894773659,
            None,
            "admin.zachwilson.tech",
            "/robots.txt",
            "2022-12-31 00:09:53.157 UTC",
        ),
        Web_Events(
            348646037,
            -2012543895,
            None,
            "www.zachwilson.tech",
            "/",
            "2023-01-01 02:11:36.696 UTC",
        ),
    ]
    fake_Web_Events_df = spark.createDataFrame(input_data_Web_Events)
    fake_Web_Events_df.createOrReplaceTempView("web_events")

    input_data_Devices_Cumulated = [
        Devices_Cumulated(
            1272828233,
            "Chrome",
            [date(2022, 12, 31)],
            date(2023, 1, 1),
        ),
        Devices_Cumulated(
            348646037,
            "Googlebot",
            [date(2023, 1, 1)],
            date(2023, 1, 1),
        ),
    ]
    Devices_Cumulated_df = spark.createDataFrame(input_data_Devices_Cumulated)
    Devices_Cumulated_df.createOrReplaceTempView("devices_cumulated")

    # Expected output
    expected_output = [
        Devices_Cumulated(
            1272828233,
            "Chrome",
            [date(2022, 12, 31), date(2023, 1, 1)],
            date(2023, 1, 1),
        ),
        Devices_Cumulated(
            348646037,
            "Googlebot",
            [date(2023, 1, 1)],
            date(2023, 1, 1),
        ),
    ]
    expected_df = spark.createDataFrame(expected_output)

    # acutal dataframe
    actual_df = job_2(spark, "Devices_Cumulated", 2022)

    # assert
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)


if __name__ == "__main__":
    pytest.main([__file__]) # type: ignore