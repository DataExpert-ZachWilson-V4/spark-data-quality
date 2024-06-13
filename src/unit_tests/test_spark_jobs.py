from chispa.dataframe_comparer import *
from jobs.job_2 import job_2, current_year
from jobs.job_1 import job_1
from collections import namedtuple
from datetime import date


devices = namedtuple("Devices", "device_id browser_type dates_active event_count")
web_events = namedtuple("WebEvents", "user_id device_id referrer host url event_time")
devices_cumulated = namedtuple(
    "DevicesCumulated", "user_id browser_type dates_active date"
)

actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")

def test_devices_cumulated_table(spark):
    # unit tests
    #  - for input tables if in web_event and data_devices the event happened in the date other than 2023-01-02
    #  - for input data_devices_cumulated if user id is None and if browser_type is None

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
    devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")

    # expected output based on our input
    expected_output = [
        devices_cumulated(
            1041379120, "Safari", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)
        ),
        devices_cumulated(
            1967566579, "Chrome", [date(2023, 1, 2), date(2023, 1, 1)], date(2023, 1, 2)
        ),
    ]
    expected_df = spark.createDataFrame(expected_output)

    # running the job
    actual_df = job_1(spark, "devices_cumulated")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_actors_table(spark):
    # unit tests
    #  - for input actor_films if year is not current_year+1
    #  - for input actors_df if last year films is not null and this year film is not null (Uma Thurman)

    input_data_actor_films = [
        actor_films(
            "Uma Thurman",
            "nm0000235",
            "Kill Bill: Volume 2",
            current_year + 1,
            8090000,
            8.0,
            "tt0378194",
        ),
        actor_films(
            "Ian McKellen",
            "nm0005212",
            "The Lord of the Rings: The Return of the King",
            current_year + 1,
            2000000,
            8.9,
            "tt0167260",
        ),
        actor_films(
            "John Dow",
            "nm0007412",
            "Math",
            current_year - 1,
            5421052,
            6.5,
            "tt0112578",
        ),
    ]
    fake_actor_films_df = spark.createDataFrame(input_data_actor_films)
    fake_actor_films_df.createOrReplaceTempView("actor_films")

    input_data_actors = [
        actor(
            "Uma Thurman",
            "nm0000235",
            [
                Row(
                    year=current_year,
                    film="Kill Bill: Vol. 1",
                    votes=1200000,
                    rating=8.1,
                    film_id="tt0266697",
                )
            ],
            "star",
            True,
            current_year,
        )
    ]
    fake_actors_df = spark.createDataFrame(input_data_actors)
    fake_actors_df.createOrReplaceTempView("actors")

    # expected output based on our input
    expected_output = [
        actor(
            "Uma Thurman",
            "nm0000235",
            [
                Row(
                    year=current_year,
                    film="Kill Bill: Vol. 1",
                    votes=1200000,
                    rating=8.1,
                    film_id="tt0266697",
                ),
                Row(
                    year=current_year + 1,
                    film="Kill Bill: Volume 2",
                    votes=8090000,
                    rating=8.0,
                    film_id="tt0378194",
                ),
            ],
            "good",
            True,
            current_year + 1,
        ),
        actor(
            "Ian McKellen",
            "nm0005212",
            [
                Row(
                    year=current_year + 1,
                    film="The Lord of the Rings: The Return of the King",
                    votes=2000000,
                    rating=8.9,
                    film_id="tt0167260",
                )
            ],
            "star",
            True,
            current_year + 1,
        ),
    ]
    expected_df = spark.createDataFrame(expected_output)

    # running the job
    actual_df = job_2(spark, "actors", current_year)

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
