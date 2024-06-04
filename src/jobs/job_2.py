from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2() -> str:
    query = """
        select
            player_name,
            team_id,
            count(*) as number_of_games,
            sum(reb) as total_rebounds,
            sum(ast) as total_assists,
            sum(blk) as total_blocks
        from
            bootcamp.nba_game_details
        group by 1, 2
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2())

def main():
    output_table_name: str = "fct_nba_game_details"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
