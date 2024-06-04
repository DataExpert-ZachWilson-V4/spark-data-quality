from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
     WITH remove_duplicates AS (
        SELECT game_id, team_id, player_id, player_name,
            ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_number
        FROM bootcamp.nba_game_details
    )
    SELECT *
    FROM remove_duplicates
    WHERE row_number = 1
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "sumanacheera.nba_game_details_duplicated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
