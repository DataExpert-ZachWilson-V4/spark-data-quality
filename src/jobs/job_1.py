from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *


def query_1(input_table_name: str) -> str:
    query = f"""
        WITH CTE AS (
        -- Assign a unique row number to each combination of game_id, team_id, and player_id
        SELECT *,
                ROW_NUMBER() OVER (PARTITION BY game_id, team_id, player_id ORDER BY dim_game_date) AS row_num
        FROM {input_table_name}
        )
        -- Select all columns except row_num from the CTE
        SELECT 
        game_id,
        team_id,
        player_id,
        dim_team_abbreviation,
        dim_player_name,
        dim_start_position,
        dim_did_not_dress,
        dim_not_with_team,
        m_seconds_played,
        m_field_goals_made,
        m_field_goals_attempted,
        m_3_pointers_made,
        m_3_pointers_attempted,
        m_free_throws_made,
        m_free_throws_attempted,
        m_offensive_rebounds,
        m_defensive_rebounds,
        m_rebounds,
        m_assists,
        m_steals,
        m_blocks,
        m_turnovers,
        m_personal_fouls,
        m_points,
        m_plus_minus,
        dim_game_date,
        dim_season,
        dim_team_did_win
        FROM CTE
        WHERE row_num = 1  -- Filter out only the rows where the row number is 1. Reason: To filter out unique records without duplicates
    """
    return query


def job_1(spark_session: SparkSession,input_table_name: str)-> Optional[DataFrame]:
  return spark_session.sql(query_1(input_table_name))
