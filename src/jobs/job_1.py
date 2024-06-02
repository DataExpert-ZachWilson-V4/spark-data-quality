from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

#
def query_1(input_table_name: str) -> str:
    return f"""
        WITH lagged AS (
        SELECT player_name,
          CASE WHEN is_active THEN 1 ELSE 0 END AS is_active,
          current_season,
          CASE WHEN LAG(is_active,1) OVER (PARTITION BY player_name ORDER BY current_season) THEN 1 ELSE 0 END AS is_active_last_season
        FROM {input_table_name}
        WHERE current_season <= 2005
        ), streaked AS (
          SELECT *,
            SUM(CASE WHEN is_active <> is_active_last_season THEN 1 ELSE 0 END) OVER (PARTITION BY player_name ORDER BY current_season) AS streak_identifier
          FROM lagged
        )
        SELECT player_name,
          MAX(is_active) = 1 AS is_active,
          MIN(current_season) AS start_season,
          MAX(current_season) AS end_season
        FROM streaked
        GROUP BY player_name,
          streak_identifier
    """
    return  query


def job_1(spark_session: SparkSession, input_df: DataFrame, input_table_name: str) -> Optional[DataFrame]:
    input_df.createOrReplaceTempView(input_table_name)
    return spark_session.sql(query_1(input_table_name))
def main():
    input_table_name: str = "nba_players"
    output_table_name: str = "nba_player_scd_merge"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    input_df = spark_session.table(input_table_name)
    output_df = job_1(spark_session, input_df, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)