from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    WITH
    previous_date_records AS (
        SELECT *
        FROM {output_table_name}
        WHERE date = DATE('2023-01-06')  
    ),

    current_date_records AS (
        SELECT e.user_id,
        d.browser_type,
        CAST(date_trunc('day', e.event_time) AS DATE) AS event_date
        FROM web_events e
        LEFT JOIN devices d ON d.device_id = e.device_id
        WHERE date_trunc('day', e.event_time) = DATE('2023-01-07')
        GROUP BY e.user_id, d.browser_type, CAST(date_trunc('day', e.event_time) AS DATE)
    )

    SELECT 
    COALESCE(p.user_id, c.user_id) AS user_id,
    COALESCE(p.browser_type, c.browser_type) AS browser_type,
    CASE 
        WHEN p.dates_active IS NOT NULL THEN ARRAY(c.event_date) || p.dates_active 
        ELSE ARRAY(c.event_date)
    END AS dates_active,
    DATE('2023-01-07') AS date
    FROM previous_date_records p
    FULL OUTER JOIN current_date_records c ON c.user_id = p.user_id AND c.browser_type = p.browser_type
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "user_devices_cummulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
