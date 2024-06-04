from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(output_table_name: str) -> str:
    query = f"""
    WITH data_ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY game_id, team_id, player_id
                ORDER BY game_id, team_id, player_id
            ) AS row_num
        FROM {output_table_name}
    )
    SELECT game_id, team_id, player_id
    FROM data_ranked
    WHERE row_num = 1
    """

    return query


def job_2(
    spark_session: SparkSession, output_table_name: str
) -> Optional[DataFrame]:

    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)

    return spark_session.sql(query_2(output_table_name))


def main():
    output_table_name: str = "nba_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )

    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
