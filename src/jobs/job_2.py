from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
    --query 2 from week 2 fact modelling
    WITH 
    -- extract data of most recent date
    yest AS (
        SELECT  
            user_id,
            browser_type,
            dates_active,
            date
        FROM 
            user_devices_cumulated
        WHERE 
            DATE = DATE('2023-01-01')
    ),
    
    -- We join `web_events` with `devices`, because we need to track daily user activity by browser type, we need both user_id and browser_type in the same table
    joined AS (
        SELECT 
            a.user_id,
            a.event_time,
            b.browser_type,
            b.os_type,
            b.device_type
        FROM 
            web_events a
        INNER JOIN 
            devices b 
        ON 
            a.device_id = b.device_id
        WHERE 
            date_trunc('day', event_time) = DATE('2023-01-01') -- Adjust the date to process the next day's events
    ),
    
    -- aggregates event data by user and browser type for today
    today AS (
        SELECT 
            user_id,
            browser_type,
            CAST(date_trunc('day', event_time) AS DATE) AS event_date,
            COUNT(1) AS count_events -- Count of events per user per browser type per day
        FROM
            joined 
        GROUP BY
            user_id,
            browser_type,
            CAST(date_trunc('day', event_time) AS DATE)
    )
    -- Append new events to old if there are any, else we create an array of today's events
    SELECT 
        COALESCE(y.user_id, t.user_id) AS user_id,
        COALESCE(y.browser_type, t.browser_type) AS browser_type,
        CASE
            WHEN y.dates_active IS NOT NULL THEN array(t.event_date) || y.dates_active
            ELSE array(t.event_date)
        END AS dates_active,
        DATE('2023-01-01') AS date
    FROM 
        yest y 
    FULL OUTER JOIN  
        today t 
    ON 
        y.user_id = t.user_id AND y.browser_type = t.browser_type
    """
    return query


def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
