from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
        WITH dedup AS (
            SELECT 
                *, 
                row_number() OVER (PARTITION BY game_id, player_id, team_id -- ROW_NUMBER is an analytical function which assign unique sequence for each row based on the combination of the partitioned columns
                    ORDER BY game_id, player_id, team_id) AS rn
            FROM {output_table_name}
        )
        -- Select all the columns except the ranked column 
        --  filter out only the first row from the group of duplicates
        SELECT
            game_id, team_id, player_id, player_name
        FROM dedup 
        WHERE rn = 1 -- Select only the first value
        and game_id = 21400714
        and team_id = 1610612746
        and player_id = 201150
        and player_name = 'Spencer Hawes'
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "nba_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
