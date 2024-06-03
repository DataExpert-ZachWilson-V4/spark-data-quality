from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(input_table_name: str) -> str:
    """Query to remove duplicates from the NBA Game Details table

    :param input_table_name: NBA Game Details table
    :type input_table_name: str
    :return: Query
    :rtype: str
    """
    query = f"""
    -- Use the ROW NUMBER function to partition the data
    -- Chose it over the DISTINCT keyword because it is more flexible and possibly faster
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
    -- SELECT only the first row of each partition
    SELECT game_id, team_id, team_abbreviation, team_city, player_id, player_name, nickname, start_position, comment, min, fgm, fga, fg_pct, fg3m, fg3a, fg3_pct, ftm, fta, ft_pct, oreb, dreb, reb, ast, stl, blk, to, pf, pts, plus_minus
    FROM duplicate_data
    WHERE rn = 1
    """
    return query


def job_2(
    spark_session: SparkSession, input_table_name: str, dataframe: DataFrame
) -> Optional[DataFrame]:
    """Remove duplicates from the NBA Game Details table

    :param spark_session: Spark session
    :type spark_session: SparkSession
    :param input_table_name: NBA Game Details table
    :type input_table_name: str
    :param dataframe: Input dataframe
    :type dataframe: DataFrame
    :return: Deduplicated dataframe output
    :rtype: Optional[DataFrame]
    """
    # Create a temporary view
    dataframe.createOrReplaceTempView(input_table_name)
    return spark_session.sql(query_2(input_table_name))


def main():
    output_table_name: str = "actors_history_scd"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
