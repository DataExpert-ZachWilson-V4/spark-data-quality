from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(input_table_name: str) -> str:
    query = f"""
    WITH duplicate_data as (
        SELECT ROW_NUMBER() OVER (
                PARTITION BY game_id,
                team_id,
                player_id
                ORDER BY game_id, team_id, player_id
            ) as rn,
            *
        FROM {input_table_name}
    )
    SELECT game_id, team_id, team_abbreviation, team_city, player_id, player_name, nickname, start_position, comment, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, to, pf, pts, plus_minus
    FROM duplicate_data
    WHERE rn = 1
    """
    return query


def job_2(
    spark_session: SparkSession, input_table_name: str, dataframe: DataFrame
) -> Optional[DataFrame]:
    dataframe.createOrReplaceTempView(input_table_name)
    return spark_session.sql(query_2(input_table_name))
