from chispa.dataframe_comparer import assert_df_equality
from jobs.job_1 import job_1
from jobs.job_2 import job_2
from collections import namedtuple
from datetime import date


def test_job_1(spark):
    # Input data
    # Define input data headers
    actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")
    actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")

    input_data = [
        actor_films("Charles Chaplin", "nm0000122", "The Kid", "1921", "115152", "8.3", "tt0012349"),
        actor_films("Milton Berle", "nm0000926", "Little Lord Fauntleroy", "1921", "283", "6.7", "tt0012397"),
        actor_films("Jackie Coogan", "nm0001067", "The Kid", "1921", "115152", "8.3", "tt0012349"),
        actor_films("Jackie Coogan", "nm0001067", "My Boy", "1921", "243", "6.2", "tt0012486"),
    ]

    input_data_df = spark.createDataFrame(input_data)
    input_data_df.createOrReplaceTempView("actor_films")

    # Expected output data based on the input
    # Define expected output 
    expected_output = [
        actors("Charles Chaplin", "nm0000122", [["The Kid", 115152, 8.3,"tt0012349", 1921 ]], "star", true, 1921),
        actors("Milton Berle", "nm0000125", [["Little Lord Fauntleroy",283,6.7,"tt0012397", 1921]], "average", true,  1921),
        actors("Jackie Coogan", "nm0001067", [["The Kid", 115152, 8.3, "tt0012349", 1921],["My Boy", 243, 6.2, "tt0012486", 1920]], "good", true, 1921)
    ]
   
    expected_output_df = spark.createDataFrame(expected_output)

    # Run Job 1
    actual_df = job_1(spark, "actors", 1921)

    # Verify that the dataframes are identical - test is passed
    assert_df_equality(actual_df, expected_output_df, ignore_nullable=True)

