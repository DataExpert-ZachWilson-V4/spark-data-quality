from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    WITH lagged AS(


Select 
    actor,
    actor_id,
    films,
    quality_class,
     CASE
       WHEN is_active Then 1
    ELSE 0
    end as is_active,
    CASE
        When LAG(is_active, 1) Over (Partition by actor_id
        Order by
        Current_year) Then 1
        Else 0
        end as is_active_last_year,
    lag(quality_class, 1) Over (Partition by Actor_id
    Order By
        current_year
    ) as quality_class_last_year,
    current_year
From {output_table_name}
WHERE current_year <= 1999
),
-- breaking out streak case statement on its own for neatness
Streaked AS(
SELECT
  *,
  SUM(CASE 
        WHEN is_active <> is_active_last_year or
        quality_class <> quality_class_last_year THEN 1 
      ELSE 0 END )
    OVER(PARTITION BY actor_id ORDER BY current_year) AS Streak_identifier
    FROM lagged
)
-- Determining how many years active as well as which years are start and end of activity 
Select 
    actor,
    actor_id, 
    MAX(quality_class) as quality_class,
    MAX(is_active) = 1 as is_active, -- this has been changed back into a boolen per WEEK 1 Lab 2
    MIN(current_year) as start_year,
    MAX(current_year) as end_year,
    2000 as current_year
    FROM streaked
    GROUP BY actor, actor_id, Streak_identifier
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "<mymah592_actors_history_scd>"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
