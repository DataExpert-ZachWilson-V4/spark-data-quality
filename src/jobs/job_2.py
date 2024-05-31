from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    """Dedupes game details"""
    query = f"""
          WITH numbered_rows --dedupes nba_players by game_id, team_id and player_id
     AS (SELECT *,
                Row_number()
                  OVER(
                    partition BY game_id, team_id, player_id ORDER BY game_id) AS row_number
         FROM   nba_game_details)
        SELECT *
        FROM   numbered_rows
        WHERE  row_number = 1
        """
    return query

def deduplicate_game_details(spark_session: SparkSession, game_details_dataframe) -> Optional[DataFrame]:
  game_details_dataframe.createOrReplaceTempView("nba_game_details")
  return spark_session.sql(query_1(""))

def main():
    output_table_name: str = "hosts_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = deduplicate_game_details(spark_session, spark_session.table('nba_game_details'))
    output_df.write.mode("overwrite").insertInto(output_table_name)
