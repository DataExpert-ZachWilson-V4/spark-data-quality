from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    with
    cte as (
        select
            *,
            ROW_NUMBER() OVER (
                PARTITION BY
                    game_id,
                    team_id,
                    player_id
            ) as dup
        from
            bootcamp.nba_game_details
    )
    select
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
    from
        cte
    where
        dup = 1
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "<output table name here>"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
