from chispa.dataframe_comparer import *
from src.jobs import job_1
from src.jobs import job_2
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType, BooleanType



inputActors = namedtuple("inputActors", "actor quality_class is_active is_active_last_year qualify_class_last_year current_year")
outputActorsHistory = namedtuple("outputActorsHistory", "actor quality_class is_active start_date end_date current_year")


def test_job_1(spark_session):
    input_table_name: str = "actors"
    output_table_name: str = "spark_actors_history"

    actual_df = job_2(spark_session, input_table_name, output_table_name)

    input_table_data = [
        inputActors("Raquel Welch" ,"bad" ,"true" , None , None ,"2014"),
        inputActors("Raquel Welch" ,"bad" ,"true" , "true" ,"average" ,"2014")
    ]

    input_df = spark_session.createDataFrame(input_table_data)


    output_actors_history = [
        outputActorsHistory("Adeel Akhtar" ,"bad" ,"true" , "2014" , "2017" ,"2021"),
        outputActorsHistory("Adrienne Barbeau" ,"bad" ,"true" , "2014" ,"2014" ,"2021")

    ]

    output_actors_history_df = spark_session.createDataFrame(output_actors_history)

    expected_df = job_2(spark_session, input_df, output_actors_history_df)

    assert_df_equality(actual_df, expected_df)


input = namedtuple("input", "user_id event_date ct")
outputCumulative = namedtuple("outputCumulative", "user_id dates_active date")


def test_job_2(spark_session):
    output_table_name: str = "user_devices_comulated"

    output_table_name_schema = StructType([
        StructField("user_id", LongType(), nullable=True),
        StructField("dates_active", ArrayType(StringType(), containsNull=True), nullable=True),
        StructField("date", StringType(), nullable=False)
    ])

    input_table_data = [
        input(1, "2023-01-01", 2),
        input(209269703, "2023-01-02", 1),
        input(133, "2023-01-01", 2)
    ]
    inputJoin_df = spark_session.createDataFrame(input_table_data)


    actual_df = job_2(spark_session, output_table_name, inputJoin_df)

    expected_data = [
        outputCumulative(1, ["2024-01-01", "2024-01-02", "2024-01-03"], "2024-01-03"),        
        outputCumulative(2, ["2024-01-01", None, "2024-01-03"], "2024-01-03"),
        outputCumulative(4, [ None, None, "2024-01-03"], "2024-01-03")
    ]
    expected_df = spark_session.createDataFrame(expected_data, output_table_name_schema)

    assert_df_equality(actual_df, expected_df)


# query_1 relates to actors


# actors = namedtuple("actors", "actor actor_id quality_class is_active current_year")

# input_data = [
#         actors(actor="Alexa Demie", actor_id="nm7051533", quality_class="good", is_active="true", current_year="2021"),
#         actors(actor="Amanda Collin", actor_id="nm4518657", quality_class="average", is_active="true", current_year="2021")

# ]

# def test_query_1(spark_session):
#     fake_input_data = spark_session.createDataFrame(input_data)

#     # test_output_table_name = "test_actors"
#     actual_output_data = job_1(spark_session, fake_input_data)

#     expected_schema = StructType([
#         StructField("actor", StringType(), True),
#         StructField("actor_id", StringType(), True),
#         StructField("quality_class", LongType(), True),
#         StructField("is_active", LongType(), True),
#         StructField("start_date", StringType(), True),
#         StructField("end_date", StringType(), True),
#         StructField("current_year", StringType(), True)
#     ])

#     assert_df_equality(actual_output_data, )
#     assert actual_output_data.schema == expected_schema
# query_2 relates to hosts