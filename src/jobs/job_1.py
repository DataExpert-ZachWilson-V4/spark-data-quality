from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(input_table_name: str) -> str:
    query = f"""
                WITH 
                ranked AS (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY game_id) AS rank_num
                FROM {input_table_name}
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
                fg_pct,
                fg3m,
                fg3a,
                fg3_pct,
                ftm,
                fta,
                ft_pct,
                oreb,
                dreb,
                reb,
                ast,
                stl,
                blk,
                to,
                pf,
                pts,
                plus_minus
            FROM 
                ranked 
            WHERE 
                rank_num = 1
    """
    return query


def job_1(spark_session: SparkSession, input_df: DataFrame, input_table_name: str) -> Optional[DataFrame]:
  #output_df = spark_session.table(output_table_name)
  input_df.createOrReplaceTempView(input_table_name)
  return spark_session.sql(query_1(input_table_name))

def main():
    input_table_name: str = "actors"
    output_table_name: str = "actors_history_scd_spark"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    input_df = spark_session.table(input_table_name)
    output_df = job_1(spark_session, input_df, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)