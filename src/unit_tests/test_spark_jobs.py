from chispa.dataframe_comparer import assert_df_equality
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple


def test_job_1(spark):
    # Input data
    # Define input data headers
    actors = namedtuple(
        "actors", "actor actor_id films quality_class is_active current_year"
    )
    actor_films = namedtuple(
        "actor_films", "actor actor_id film year votes rating film_id"
    )

    input_data = [
        actor_films(
            "Charles Chaplin",
            "nm0000122",
            "The Kid",
            "1921",
            "115152",
            "8.3",
            "tt0012349",
        ),
        actor_films(
            "Milton Berle",
            "nm0000926",
            "Little Lord Fauntleroy",
            "1921",
            "283",
            "6.7",
            "tt0012397",
        ),
        actor_films(
            "Jackie Coogan",
            "nm0001067",
            "The Kid",
            "1921",
            "115152",
            "8.3",
            "tt0012349",
        ),
        actor_films(
            "Jackie Coogan", "nm0001067", "My Boy", "1921", "243", "6.2", "tt0012486"
        ),
    ]

    input_data_df = spark.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("actor_films")

    # Expected output data based on the input
    # Define expected output
    expected_output = [
        actors(
            "Charles Chaplin",
            "nm0000122",
            [["The Kid", 115152, 8.3, "tt0012349", 1921]],
            "star",
            True,
            1921,
        ),
        actors(
            "Milton Berle",
            "nm0000125",
            [["Little Lord Fauntleroy", 283, 6.7, "tt0012397", 1921]],
            "average",
            True,
            1921,
        ),
        actors(
            "Jackie Coogan",
            "nm0001067",
            [
                ["The Kid", 115152, 8.3, "tt0012349", 1921],
                ["My Boy", 243, 6.2, "tt0012486", 1920],
            ],
            "good",
            True,
            1921,
        ),
    ]

    expected_output_df = spark.createDataFrame(expected_output)

    # Run Job 1
    actual_df = job_1(spark, "actors", 1921)

    # Verify that the dataframes are identical - test is passed
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)


def test_job_2(spark):
    # Input data
    # Define input data headers
    web_event = namedtuple(
        "web_event", "user_id device_id referrer host url event_time"
    )
    device = namedtuple("device", "device_id browser_type os_type device_type")
    user_devices_cumulated = namedtuple(
        "user_devices_cumulated", "user_id browser_type dates_active date"
    )

    # Web input
    web_input_data = [
        web_event(
            -838968542,
            -1259816303,
            "http://zachwilson.tech",
            "www.zachwilson.tech",
            "/",
            "2021-03-20 01:48:55.001 UTC",
        ),
        web_event(
            972269998,
            -1259816303,
            "http://zachwilson.tech",
            "www.zachwilson.tech",
            "/contact",
            "2021-03-20 01:48:56.298 UTC",
        ),
        web_event(
            972269998,
            -1259816303,
            "https://www.zachwilson.tech/contact",
            "www.zachwilson.tech",
            "/api/v1/contact",
            "2021-03-20 01:51:23.533 UTC",
        ),
        web_event(
            -623801372,
            532630305,
            None,
            "www.zachwilson.tech",
            "/api/v1/spark-post",
            "2021-03-20 01:51:37.091 UTC",
        ),
        web_event(
            -1728795220,
            464202459,
            None,
            "www.zachwilson.tech",
            "/",
            "2021-03-20 00:54:19.715 UTC",
        ),
        web_event(
            -1728795220,
            464202459,
            None,
            "www.zachwilson.tech",
            "/",
            "2021-03-20 00:54:21.596 UTC",
        ),
    ]

    web_event_df = spark.createDataFrame(web_input_data)
    web_event_df.createOrReplaceTempView("web_event")

    # Device input
    device_input_data = [
        device(-1259816303, "QQ Browser", "Windows", "Other"),
        device(532630305, "Other", "Other", "Other"),
        device(464202459, "Chrome", "Windows", "Other"),
    ]

    devices_df = spark.createDataFrame(device_input_data)
    devices_df.createOrReplaceTempView("device")

    # Expected output data based on the input
    # Define expected output
    expected_output = [
        user_devices_cumulated(-838968542, "QQ Browser", ["2021-03-20"], "2021-03-20"),
        user_devices_cumulated(972269998, "QQ Browser", ["2021-03-20"], "2021-03-20"),
        user_devices_cumulated(-623801372, "Other", ["2021-03-20"], "2021-03-20"),
        user_devices_cumulated(-1728795220, "Chrome", ["2021-03-20"], "2021-03-20"),
    ]

    expected_output_df = spark.createDataFrame(expected_output)

    # Run Job 2
    actual_df = job_2(spark, "user_devices_cumulated", "2021-03-20")

    # verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)
