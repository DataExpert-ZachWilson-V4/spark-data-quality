from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def User_Devices_Cumulated(output_table_name: str) -> str:
    query = f"""
WITH yesterday AS (
    SELECT *
    FROM {output_table_name}
    WHERE date = DATE('2023-01-02')
),
today AS (
    SELECT
        we.user_id,
        d.browser_type,
        CAST(DATE_TRUNC('day', we.event_time) AS DATE) AS event_date
    FROM 
        bootcamp.web_events we
        LEFT JOIN bootcamp.devices d 
        ON we.device_id = d.device_id
    WHERE 
        CAST(DATE_TRUNC('day', we.event_time) AS DATE) = DATE('2023-01-03')
    GROUP BY
        we.user_id,
        d.browser_type,
        CAST(DATE_TRUNC('day', we.event_time) AS DATE)
)
SELECT 
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.browser_type, t.browser_type) AS browser_type,
    CASE WHEN y.dates_active IS NOT NULL THEN ARRAY[t.event_date] || y.dates_active
        ELSE ARRAY[t.event_date]
    END AS dates_active,
    DATE('2023-01-03') AS date
FROM 
    yesterday y
    FULL OUTER JOIN today t 
    ON y.user_id = t.user_id AND y.browser_type = t.browser_type
    """
    return query

def Update_User_Devices_Cumulated(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(User_Devices_Cumulated(output_table_name))

def main():
    output_table_name: str = "danieldavid.user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("Update_User_Devices_Cumulated")
        .getOrCreate()
    )
    output_df = Update_User_Devices_Cumulated(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
