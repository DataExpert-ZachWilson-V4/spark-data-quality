from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
     with base as 
  ( 
    SELECT 
        game_id,
        team_id,
        team_abbreviation,
        team_city,
        player_id player_id,
        player_name,
        start_position,
        comment LIKE '%DND%' AS dim_did_not_dress,
        comment LIKE '%NWT%' AS dim_not_with_team,
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
        plus_minus,
        ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) AS dup_row
        from 
        bootcamp.nba_game_details
  )
  select 
  game_id,
        team_id,
        team_abbreviation,
        team_city,
        player_id player_id,
        player_name,
        start_position,
        dim_did_not_dress,
        dim_not_with_team,
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
  from 
  base 
  where dup_row =1 
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "nba_stats_dedup"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
