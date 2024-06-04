from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)
import shutil
import os

schema = StructType(
    [
        StructField("game_id", LongType(), True),
        StructField("team_id", LongType(), True),
        StructField("team_abbreviation", StringType(), True),
        StructField("team_city", StringType(), True),
        StructField("player_id", LongType(), True),
        StructField("player_name", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField("start_position", StringType(), True)
    ]
)


def create_output_table(spark_session, table_name):
    
    spark_session.sql(f"DROP TABLE IF EXISTS {table_name}")
    # Get the warehouse location
    warehouse_location = spark_session.conf.get("spark.sql.warehouse.dir", "spark-warehouse")

    # Construct the table folder path
    table_folder_path = os.path.join(warehouse_location, table_name)
    print("table_folder_path", table_folder_path)
    
    # Check if the folder exists and delete it
    if os.path.exists(table_folder_path):
        shutil.rmtree(table_folder_path)
        print(f"Table folder '{table_folder_path}' has been deleted.")
    else:
        print(f"Table folder '{table_folder_path}' does not exist.")
        tables = spark_session.catalog.listTables()
        print(f"Tables in the default database after dropping {table_name}:")

    tables = spark_session.catalog.listTables()   
    for table in tables:
        print(table.name)

    if table_name not in tables:
        empty_df = spark_session.createDataFrame([], schema)
        empty_df.write.mode("overwrite").saveAsTable(table_name)   
    return spark_session.table(table_name)


def query_1(input_table_name: str) -> str:
    query = f"""
        WITH
            row_nums AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            game_id,
                            team_id,
                            player_id
                    ORDER BY game_id) AS row_number
                FROM
                    {input_table_name}
            )
        SELECT
            game_id,
            team_id,
            team_abbreviation,
            team_city,
            player_id,
            player_name,
            nickname,
            start_position
        FROM
            row_nums
        WHERE
            row_number = 1
    """
    return query


def job_1(
    spark_session: SparkSession, input_table_name: str, output_table_name: str) -> Optional[DataFrame]:
    output_df = create_output_table(spark_session, output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(input_table_name))


def main():
    input_table_name: str = "nba_game_details"
    output_table_name: str = "nba_game_dedupe"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, input_table_name, output_table_name)
    output_df.write.option(
        "path", spark_session.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    ).mode("overwrite").insertInto(output_table_name)
