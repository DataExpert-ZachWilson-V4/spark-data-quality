from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table_name: str) -> str:
    query = f""" -- Homework 2 - Query 1
    WITH rn_marking AS (
        SELECT
            *,
            CAST(ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id, team_id, player_id) AS LONG) AS rn
        FROM {input_table_name}
    )
    SELECT *
    FROM rn_marking
    WHERE rn = 1
    """
    return query

def job_1(spark_session: SparkSession, in_dataframe: DataFrame) -> Optional[DataFrame]:
    input_table = 'fct_nba_game_details'
    in_dataframe.createOrReplaceTempView(input_table)
    return spark_session.sql(query_1(input_table))

def main():
    output_table_name: str = "jsgomez14.fct_nba_game_details_deduped"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, spark_session.table(f"jsgomez14.fct_nba_game_details"))
    output_df.write.mode("overwrite").insertInto(output_table_name)
