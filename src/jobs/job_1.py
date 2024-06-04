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

def query_1(output_table_name: str) -> str:
schema = StructType(
    [
        StructField("game_id", LongType(), True),
        StructField("team_id", LongType(), True),
        StructField("team_abbreviation", StringType(), True),
        StructField("team_city", StringType(), True),
        StructField("player_id", LongType(), True),
        StructField("player_name", StringType(), True),
        StructField("nickname", StringType(), True),
        StructField("start_position", StringType(), True),
        StructField("comment", StringType(), True),
        StructField("min", StringType(), True),
        StructField("fgm", DoubleType(), True),
        StructField("fga", DoubleType(), True),
        StructField("fg_pct", DoubleType(), True),
    ]
)

def load_or_create_table(spark_session, table_name):
    if table_name not in spark_session.catalog.listTables():
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
            start_position,
            comment,
            min,
            fgm,
            fga,
            fg_pct
        FROM
            row_nums
        WHERE
            row_number = 1
    """
    return query
def job_1(
    spark_session: SparkSession, input_table_name: str, output_table_name: str
) -> Optional[DataFrame]:
    output_df = load_or_create_table(spark_session, output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(input_table_name))


def main():
    input_table_name: str = "nba_game_details"
    output_table_name: str = "nba_game_details_dedup"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, input_table_name, output_table_name)
    output_df.write.option(
        "path", spark_session.conf.get("spark.sql.warehouse.dir", "spark-warehouse")
    ).mode("overwrite").insertInto(output_table_name)
