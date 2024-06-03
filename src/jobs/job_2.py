from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

def query_2(input_table_name: str) -> str:
    query = f"""
    -- Deduplicates NBA game details with CTE that selects the first row from each partition of (game_id, team_id, player_id) 
    WITH deduped_nba_game_details AS (
        SELECT
            ROW_NUMBER() OVER (
                PARTITION BY
                    game_id,
                    team_id,
                    player_id
                ORDER BY game_id, team_id, player_id -- Add ORDER BY clause here
            ) AS row_number,
            *
        FROM {input_table_name}
    )
    SELECT dgd.*
    FROM deduped_nba_game_details dgd
    WHERE row_number = 1
    """
    return query

def job_2(spark_session: SparkSession, input_df: DataFrame):
    input_table_name = "nba_game_details"
    input_df.createOrReplaceTempView(input_table_name)
    input_df = spark_session.sql(query_2(input_table_name)) 
    input_df = input_df.withColumn("row_number", col("row_number").cast("long"))
    return input_df

def main():
    output_table_name: str = "amaliah21315.nba_game_details_dedupped"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, spark_session.table(output_table_name))
    output_df.write.mode("overwrite").insertInto(output_table_name)