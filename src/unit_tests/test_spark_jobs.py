from chispa.dataframe_comparer import *
from ..jobs.job_1 import current_year, job_1
from collections import namedtuple

actor_films = namedtuple("ActorFilms", "actor actor_id film year votes rating film_id")
actor = namedtuple("Actor", "actor actor_id films quality_class is_active current_year")

def test_actors_acc(spark):

    # Creating fake input for actor_films table
    input_data = [
        actor_films("Fred Astaire", "nm0000001", "Ghost Story", 1981, 7731, 6.3, "tt0082449"),
        actor_films("Fred Astaire", "nm0000001", "Shall We Dance", 1937, 6603, 7.5, "tt0029546"),
        actor_films("Paul Newman", "nm0000056", "Where the Money Is", current_year +1 ,
                     5747, 6.2, "tt0149367"),
        actor_films("John Cleese", "nm0000092", "Isn't She Great", current_year+1,
                     2281, 5.5, "tt0141399"),
        actor_films("John Cleese", "nm0000092", "The Magic Pudding", current_year+1,
                     428, 5.9, "tt0141399")
    ]

    # Generating the actor_films data based on the fake input
    input_data = spark.createDataFrame(input_data)
    input_data.createOrReplaceTempView("bootcamp.actor_films")

    # Expected output based on our input
    # Since this job starts with and empty table,
    # than it filters the year by the current year + 1,
    # it must return two rows, one for each actor that made films in the current year + 1 date
    expected_output = [
        actor("Paul Newman", "nm0000056", [["Where the Money Is", 5747, 6.2,  "tt0149367", current_year +1]],
               "average" , True , current_year + 1),
        actor("John Cleese", "nm0000092", [["Isn't She Great"], 2281, 5.5,  "tt0141399", current_year +1,
                                            ["The Magic Pudding", 428, 5.9,  "tt0141399", current_year +1]],
                                              "bad", True, current_year+1)
    ]

    expected_df = spark.createDataFrame(expected_output)

    # Running our actual job
    actual_df = job_1(spark, "barrocaeric.actors", current_year)

    # Verifying that the dataframes are identical
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)