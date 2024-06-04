from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table_name: str) -> str:
    query = f"""
        WITH CTE AS (
        SELECT *,
        ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS row_num FROM {input_table_name}
        )
        SELECT 
        game_id,
        team_id,
        player_id,
        m_field_goals_made,
        dim_team_did_win 
        FROM CTE WHERE row_num = 1 
        """
    return query

def job_1(spark_session: SparkSession, input_table_name: str) -> Optional[DataFrame]:
  input_df = spark_session.table(input_table_name)
  input_df.createOrReplaceTempView(input_table_name)
  return spark_session.sql(query_1(input_table_name))

def main():
    input_table_name: str = "fct_nba_game_details"
    output_table_name: str = "deduped_nba_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, input_table_name, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
