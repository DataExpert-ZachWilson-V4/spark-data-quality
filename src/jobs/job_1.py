from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


# The query from week2-Query1 to remove duplication in data
def query_1(input_table_name: str) -> str:
    query = f"""
    select
        game_id
        , team_id
        , player_id
        , team_abbreviation
        , team_city
        , nickname
        , player_name
    FROM
    (
        SELECT 
            game_id
            , team_id
            , player_id
            , team_abbreviation
            , team_city
            , nickname
            , player_name
            ,row_number() over ( PARTITION BY game_id,team_id,player_id order by player_id) rnk
        FROM {input_table_name}
    )
    where rnk = 1
    """
    return query


def job_1(
    spark_session: SparkSession, input_table_name: str, input_dataframe: DataFrame
) -> Optional[DataFrame]:
    input_dataframe.createOrReplaceTempView(input_table_name)
    return spark_session.sql(query_1(input_table_name))


def main():
    input_table_name: str = "nba_game_details"
    output_table_name: str = "nba_game_details_deduped"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    input_dataframe = spark_session.table(input_table_name)
    output_df = job_1(spark_session, input_table_name, input_dataframe)
    output_df.write.mode("overwrite").insertInto(output_table_name)
