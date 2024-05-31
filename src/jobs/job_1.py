from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    """Cumulates host vists on a daily basis incrementally"""
    query = f"""
    WITH yesterday AS (
    SELECT 
    * 
    FROM 
    hosts_cumulated 
        WHERE 
    date = '2023-01-01'
    ), 
    -- Creating a CTE for today's data
    today AS (
    SELECT 
        host, 
        DATE(event_time) as date, 
        COUNT(1) as event_count
    FROM 
        web_events 
    WHERE 
        DATE(event_time) = '2023-01-02'
    GROUP BY 
        host, 
        DATE(event_time)
    )

    -- Select from the joined CTEs
    SELECT 
    COALESCE(today.host, yesterday.host) as host, 
    CASE 
        WHEN yesterday.host_activity_datelist IS NULL THEN array(today.date) 
        ELSE array_union(array(today.date), yesterday.host_activity_datelist) 
    END as host_activity_datelist, 
    COALESCE(today.date, date_add(yesterday.date, 1)) as date 
    FROM 
    today 
    FULL OUTER JOIN 
    yesterday 
    ON 
    today.host = yesterday.host
        """
    return query

def incremental_update_host_cumulated(spark_session: SparkSession, hosts_cumulated_dataframe, web_events_dataframe) -> Optional[DataFrame]:
  hosts_cumulated_dataframe.createOrReplaceTempView("hosts_cumulated")
  web_events_dataframe.createOrReplaceTempView("web_events")
  return spark_session.sql(query_1(""))

def main():
    output_table_name: str = "hosts_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = incremental_update_host_cumulated(spark_session, spark_session.table('hosts_cumulated'), spark_session.table('web_events'))
    output_df.write.mode("overwrite").insertInto(output_table_name)
