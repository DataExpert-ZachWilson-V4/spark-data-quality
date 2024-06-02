from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1() -> str:
    query = f"""
    with
        nba_game_details_with_row_num as (
            select
                row_number() over (
                    partition by
                        game_id,
                        team_id,
                        player_id
                    order by 
                        game_id,
                        team_id,
                        player_id
                ) as row_num,
                *
            from
                nba_game_details
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
        fg3m,
        fg3a,
        ftm,
        fta,
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
        nba_game_details_with_row_num
    where
        row_num = 1
    """
    return query


def job_1(spark_session: SparkSession, dataframe: DataFrame) -> Optional[DataFrame]:
    dataframe.createOrReplaceTempView("nba_game_details")
    return spark_session.sql(query_1())


def main():
    output_table_name: str = "nba_game_details_deduped"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, spark_session.table("nba_game_details"))
    output_df.write.mode("overwrite").insertInto(output_table_name)
