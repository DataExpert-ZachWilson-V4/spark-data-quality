from chispa.dataframe_comparer import *
from jobs.job_1 import job_1
from collections import namedtuple
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    ArrayType,
    BooleanType,
)

Actor = namedtuple(
    "Actor", "actor actor_id average_rating quality_class is_active current_year"
)

ActorSCD = namedtuple(
    "ActorSCD",
    "actor actor_id is_active average_rating quality_class start_date, end_date, current_year",
)

input_actors_data = [
    Actor(
        actor="Clancy Brown",
        actor_id="nm0000317",
        average_rating=5.60,
        quality_class="bad",
        is_active=True,
        current_year=2012,
    ),  
    Actor(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        average_rating=6.3,
        quality_class="average",
        is_active=True,
        current_year=2012,
    ),
]

expected_actorscd_output = [
    ActorSCD(
        actor="Clancy Brown",
        actor_id="nm0000317",
        is_active=True,
        average_rating=5.60,
        quality_class="bad",
        start_date=2012,
        end_date=2012,
        current_year=2012,
    ),
      ActorSCD(
        actor="Kevin Bacon",
        actor_id="nm0000102",
        is_active=True,
        average_rating=6.3,
        quality_class="average",
        start_date=2012,
        end_date=2012,
        current_year=2012,
    ),
]

actors_schema = StructType(
    [
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("average_rating", DoubleType(), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), True),
        StructField("current_year", IntegerType(), False),
    ]
)

actorscd_schema = StructType(
    [
        StructField("actor", StringType(), True),
        StructField("actor_id", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("average_rating", DoubleType(), False),
        StructField("quality_class", StringType(), False),
        StructField("start_date", IntegerType(), True),
        StructField("end_date", IntegerType(), True),
        StructField("current_year", IntegerType(), False),
    ]
)


# Define the schema for the films array
def test_spark_queries_1(spark_session):

    input_actors_dataframe = spark_session.createDataFrame(
        input_actors_data, actors_schema
    )
    input_actors_dataframe.orderBy("actor")
    actual_dataframe = job_1(spark_session, input_actors_dataframe)

    expected_df = spark_session.createDataFrame(expected_actorscd_output, actorscd_schema)
    
    expected_df.orderBy("actor")
    assert_df_equality(actual_dataframe, expected_df, ignore_nullable=True)
