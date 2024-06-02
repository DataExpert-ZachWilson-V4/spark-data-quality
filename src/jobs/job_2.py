from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    with rn_cte as(
  select
  game_id, team_id, player_id,
  row_number() over (partition by game_id, team_id, player_id) as row_count
  from
  derekleung.nba_game_details
  )
--Step 2: Note for every row in the CTE above with row_count > 1 is a duplicate according to our filtering criteria
--So we could use a WHERE clause to take them out
select 
  game_id, team_id, player_id, row_count
from
  rn_cte
where
  row_count = 1
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "nba_game_details_deduped"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
