from chispa.dataframe_comparer import *
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType

# job 1
inputActors = namedtuple("inputActors",
                         "actor quality_class is_active is_active_last_year quality_class_last_year current_year")
outputActorsHistory = namedtuple("outputActorsHistory",
                                 "actor quality_class is_active start_date end_date current_year")


def test_job_1(spark_session):
    input_table_name: str = "actors"  # base table
    output_table_name: str = "spark_actors_history"  # write to table

    actual_df = job_1(spark_session, input_table_name, output_table_name)

    input_table_data = [
        inputActors("Charles Chaplin", "average", "true", None, None, "1914"),
        inputActors("Milton Berle", "average", "true", None, None, "1914")
    ]

    input_df = spark_session.createDataFrame(input_table_data)

    output_actors_history = [
        outputActorsHistory("Charles Chaplin", "average", "true", "1914", "1914", "2021"),
        outputActorsHistory("Milton Berle", "average", "true", "1914", "1914", "2021")

    ]

    output_actors_history_df = spark_session.createDataFrame(output_actors_history)

    expected_df = job_1(spark_session, input_df, output_actors_history_df)

    assert_df_equality(actual_df, expected_df)


# testing job 2
input = namedtuple("input", "user_id event_date ct")
outputCumulative = namedtuple("outputCumulative", "user_id dates_active date")


def test_job_2(spark_session):
    output_table_name: str = "user_devices_comulated"  # base table

    output_table_name_schema = StructType([
        StructField("user_id", LongType(), nullable=True),
        StructField("dates_active", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("date", StringType(), nullable=False)
    ])

    input_table_data = [
        input(-580833462, "2023-01-01", 1),
        input(-222720132, "2023-01-01", 1),
        input(-509308341, "2023-01-01", 2)
    ]
    inputJoin_df = spark_session.createDataFrame(input_table_data)

    actual_df = job_2(spark_session, output_table_name, inputJoin_df)

    expected_data = [
        outputCumulative(-112867796, ["2023-01-01"], "2023-01-01"),
        outputCumulative(-94037775, ["2023-01-01"], "2023-01-01"),
        outputCumulative(94331299, ["2023-01-01"], "2023-01-01")
    ]
    expected_df = spark_session.createDataFrame(expected_data, output_table_name_schema)

    assert_df_equality(actual_df, expected_df)
