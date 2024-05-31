from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    query = f"""
        --assigns numbers to repeat rows according to first occurrence
        WITH
            row_nums AS (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            game_id,
                            team_id,
                            player_id
                    ORDER BY game_id) AS row_number
                FROM
                    {output_table_name}
            )
        --select all columns except the row number
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
            row_nums
        --keep only the first occurrence for duplicate records
        WHERE
            row_number = 1
    """
    return query


def job_1(
    spark_session: SparkSession, output_table_name: str, dataframe
) -> Optional[DataFrame]:
    dataframe.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(output_table_name))


def main():
    output_table_name: str = "nba_game_details_dedup"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
