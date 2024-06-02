from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from datetime import datetime, timedelta

def query_2(output_table_name: str, date_to_load: str) -> str:
    query = f"""WITH previous AS (SELECT * FROM user_devices_cumulated WHERE date = DATE('{(datetime.strptime(date_to_load, '%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')}')), now AS (SELECT user_id, browser_type, DATE(date_trunc('day', event_time)) AS event_date FROM web_events we JOIN devices d ON d.device_id = we.device_id WHERE DATE(date_trunc('day', event_time)) = DATE('{date_to_load}') GROUP BY user_id, browser_type, DATE(date_trunc('day', event_time))) SELECT COALESCE(p.user_id, n.user_id) AS user_id, COALESCE(p.browser_type, n.browser_type) AS browser_type, CASE WHEN p.dates_active is NULL THEN array(n.event_date) ELSE concat(array(n.event_date), p.dates_active) END AS dates_active, DATE('{date_to_load}') AS date FROM previous p FULL OUTER JOIN now n ON p.user_id = n.user_id AND p.browser_type = n.browser_type"""
    return query

def job_2(spark_session: SparkSession, output_table_name: str, date_to_load: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name, date_to_load))

def main(date_to_load: str):
    output_table_name: str = "user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name, date_to_load)
    output_df.write.mode("overwrite").insertInto(output_table_name)
