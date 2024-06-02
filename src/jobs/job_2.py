from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str, input_table_name: str) -> str:
    query = f"""
WITH yesterday AS (

  SELECT
  *
  FROM {output_table_name}
  WHERE date = DATE('2022-12-31')
),

today AS (

  SELECT 
    user_id,
    CAST(date_trunc('day', event_time) AS DATE) AS event_date, --transforming ts
    COUNT(1) as ct

  FROM {input_table_name}
  
  WHERE date_trunc('day', event_time) = DATE('2023-01-01') -- first day of data
  GROUP BY 
  user_id,
  CAST(date_trunc('day', event_time) AS DATE)
  
)

SELECT 
    COALESCE(y.user_id, t.user_id) AS user_id,
    CASE
        WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
        ELSE ARRAY[t.event_date]
    END AS dates_active,
    DATE('2023-01-01') AS date 


FROM yesterday y
    FULL OUTER JOIN today t -- to ensure new users make it to the table
    ON y.user_id = t.user_id
    """
    return query

## output_table_name = cummulative table not really the true outout
## input_table_name = the true source of data from the bootcamp schema

def job_2(spark_session: SparkSession, output_table_name: str, input_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name, input_table_name))

def main():
    output_table_name: str = "user_devices_comulated"
    input_table_name: str = "web_events"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    
    output_df = job_2(spark_session, output_table_name, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
