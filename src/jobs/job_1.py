from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    query = \
        f"""
            with 
                nba_game_details_dedup as (
                    select *, 
                        ROW_NUMBER() over (PARTITION BY game_id, team_id, player_id) as row_number
                    from {output_table_name}
                ),
            select * from nba_game_details_dedup where row_number = 1
        """

    return query


def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))


def main():
    output_table_name: str = "bootcamp.nba_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
