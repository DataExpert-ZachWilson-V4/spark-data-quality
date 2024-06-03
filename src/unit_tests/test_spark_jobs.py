from chispa.dataframe_comparer import *
from jobs.job_2 import job_2, current_year
from jobs.job_1 import job_1
from collections import namedtuple
from datetime import date

# Test 1 setup
actor_films = namedtuple("ActorFilms", "actor, actor_id, film, year, votes, rating, film_id")
actor = namedtuple("Actor", "actor, actor_id, films, quality_class, is_active, current_year")

# Test 2 setup
devices = namedtuple("Devices", "device_id, browser_type, dates_active, event_count")
web_events = namedtuple("WebEvents", "user_id, device_id, referrer, host, url, event_time")
devices_cumulated = namedtuple(
    "DevicesCumulated", "user_id browser_type dates_active date"
)


def test_job1(spark):
    current_year = 2010

    input_data_actor_films = [
        actor_films(
            "Brad Pitt", "nm0000093", "Megamind", 2010, 231290, 7.2, "tt1001526"
        ),
        actor_films(
            "Pamela Anderson",
            "nm0000097",
            "Costa Rican Summer",
            2010,
            700,
            2.7,
            "tt1370426",
        ),
        actor_films(
            "Jennifer Aniston",
            "nm0000098",
            "The Bounty Hunter",
            2010,
            122138,
            5.6,
            "tt1038919",
        ),
    ]
    fake_actor_films_df = spark.createDataFrame(input_data_actor_films)
    fake_actor_films_df.createOrReplaceTempView("actor_films")

    input_data_actors = [
        actor(
            "Brad Pitt",
            "nm0000093",
            [
                Row(
                    year=current_year,
                    film="Inglourious Basterds",
                    votes=1285359,
                    rating=8.3,
                    film_id="tt0361748",
                )
            ],
            "star",
            True,
            current_year,
        ),
    ]
    fake_actors_df = spark.createDataFrame(input_data_actors)
    fake_actors_df.createOrReplaceTempView("actors")

    # expected output based on our input
    expected_output = [
        actor(
            "Brad Pitt",
            "nm0000093",
            [
                Row(
                    year=current_year,
                    film="Inglourious Basterds",
                    votes=1285359,
                    rating=8.3,
                    film_id="tt0361748",
                ),
                Row(
                    year=current_year,
                    film="Megamind",
                    votes=231290,
                    rating=7.2,
                    film_id="tt1001526",
                ),
            ],
            "good",
            True,
            current_year + 1,
        ),
        actor(
            "Pamela Anderson",
            "nm0005212",
            [
                Row(
                    year=current_year + 1,
                    film="Costa Rican Summer",
                    votes=700,
                    rating=2.7,
                    film_id="tt1370426",
                )
            ],
            "bad",
            True,
            current_year + 1,
        ),
        actor(
            "Jennifer Aniston",
            "nm0000098",
            [
                Row(
                    year=current_year + 1,
                    film="The Bounty Hunter",
                    votes=122138,
                    rating=5.6,
                    film_id="tt1038919",
                )
            ],
            "bad",
            True,
            current_year + 1,
        ),
    ]
    expected_df = spark.createDataFrame(expected_output)

    actual_df = job_2(spark, "actors", current_year)

    # Assert
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job2(spark):

    input_data_devices = [
        devices(-1138341683, "Chrome", [date(2023, 1, 2)], 1),
        devices(1967566123, "Safari", [date(2023, 1, 2)], 2),
        devices(328474741, "Mobile Safari", [date(2022, 12, 31)], 3),
    ]
    fake_devices_df = spark.createDataFrame(input_data_devices)
    fake_devices_df.createOrReplaceTempView("devices")

    input_data_web_events = [
        web_events(
            1967566579,
            -1138341683,
            "",
            "www.eczachly.com",
            "/",
            "2023-01-02 21:57:37.422 UTC",
        ),
        web_events(
            1041379120,
            1967566123,
            None,
            "www.eczachly.com",
            "/lessons",
            "2023-01-02 08:01:51.009 UTC",
        ),
        web_events(
            -1041379335,
            328474741,
            "",
            "admin.zachwilson.tech",
            "/",
            "2022-12-31 08:01:51.009 UTC",
        ),
    ]
    fake_web_events_df = spark.createDataFrame(input_data_web_events)
    fake_web_events_df.createOrReplaceTempView("web_events")

    input_data_devices_cumulated = [
        devices_cumulated(1967566579, "Chrome", [date(2023, 1, 1)], date(2023, 1, 1)),
        devices_cumulated(1041379120, None, [date(2023, 1, 1)], date(2023, 1, 1)),
    ]
    devices_cumulated_df = spark.createDataFrame(input_data_devices_cumulated)
    devices_cumulated_df.createOrReplaceTempView("devices_cumulated")

    # Expected output
    expected_output = [
        devices_cumulated(
            1041379120, "Safari", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)
        ),
        devices_cumulated(
            1967566579, "Chrome", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)
        ),
    ]
    expected_df = spark.createDataFrame(expected_output)

    # acutal dataframe
    actual_df = job_1(spark, "devices_cumulated")

    # assert
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)