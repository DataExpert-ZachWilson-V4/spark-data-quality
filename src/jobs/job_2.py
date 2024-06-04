from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(input_table_name: str) -> str:
    query = f"""
    WITH counted_rows AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id, team_id, player_id) as row_num
        FROM
            {input_table_name}
        ORDER BY
            row_num DESC
    )
    SELECT -- reduced columns because they are not necessary in understanding what is going on here
        game_id, team_id, player_id, team_abbreviation, team_city, player_name, nickname, min, start_position, fgm, fga
    FROM
        counted_rows
    WHERE
        row_num = 1
    """
    return query

def job_2(spark_session: SparkSession, input_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(input_table_name)
  output_df.createOrReplaceTempView(input_table_name)
  return spark_session.sql(query_2(input_table_name))

def main():
    input_table_name: str = "bootcamp.nba_game_details"
    output_table_name: str = "deduped_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
