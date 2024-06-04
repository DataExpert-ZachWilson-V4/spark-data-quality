from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    <YOUR QUERY HERE - using query 3 from fact data model submission>
    < create CTE to hold yesterday's data>
WITH
  old_data AS (
  SELECT * FROM {output_table_name}
  WHERE DATE = DATE('2021-01-17')
  ),

   <CTE to hold current day's load>

  current_day AS (
    SELECT web.user_id as user_id,device.browser_type as browser_type,
   CAST(date_trunc('day', web.event_time) AS DATE) AS event_date
    FROM bootcamp.web_events web LEFT JOIN bootcamp.devices device ON web.device_id = device.device_id
    WHERE date_trunc('day', web.event_time) = DATE('2021-01-18') -- 2021-01-18 as current day
    GROUP BY user_id,browser_type,CAST(date_trunc('day', event_time) AS DATE)
   )
   
 <Main Query>
SELECT
  COALESCE(od.user_id, cd.user_id) AS user_id,
  COALESCE(od.browser_type, cd.browser_type) AS browser_type,
<if user is active yesterday, concat the dates_Active to current date array.>
  CASE WHEN od.dates_active IS NOT NULL THEN ARRAY[cd.event_date] || od.dates_active ELSE ARRAY[cd.event_date]
  END AS dates_active,
<setting current day parameter as DATE Column >
  DATE('2021-01-18') AS DATE
FROM old_data od FULL OUTER JOIN current_day cd 
  ON od.user_id = cd.user_id 

    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "<user_devices_cumulated>"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
