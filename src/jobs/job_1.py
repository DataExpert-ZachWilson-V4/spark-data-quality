from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1() -> str:
    query = f"""
    SELECT
        player_name,
        team_id,
        COUNT(1) AS number_of_games,
        SUM(reb) AS total_rebounds,
        SUM(ast) AS total_assists
    FROM
        nba_game_details
    GROUP BY
        player_name,
        team_id
    """
    return query

def job_1(spark_session: SparkSession, dataframe) -> Optional[DataFrame]:
    dataframe.createOrReplaceTempView("nba_game_details")
    return spark_session.sql(query_1())

def main():
    output_table_name: str = "fct_nba_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, spark_session.table("nba_game_details"))
    output_df.write.mode("overwrite").insertInto(output_table_name)
if __name__ == '__main__':
    main()
    