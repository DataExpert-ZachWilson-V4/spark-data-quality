from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str, base_table: str, date: str) -> str:
    query = f"""
           WITH yesterday AS (
                SELECT * FROM {output_table_name} WHERE date = DATE_SUB(CAST('{date}' AS DATE), 1)
        ),
        today AS (
            SELECT host,CAST(DATE_TRUNC('day', event_time) AS DATE) AS date,COUNT(1) FROM {base_table} WHERE DATE_TRUNC('day', event_time) = DATE('{date}') GROUP BY host,DATE_TRUNC('day', event_time)
        )
        SELECT
        COALESCE(y.host, t.host) AS host,
        CASE
            WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY(t.date) || y.host_activity_datelist
            ELSE ARRAY(t.date) END AS host_activity_datelist,
            DATE('{date}') AS date
        FROM yesterday AS y FULL OUTER JOIN today AS t ON y.host = t.host 
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str,base_table: str,date: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name,base_table,date))

def main():
    output_table_name: str = "hosts_cumulated"
    base_table: str = 'web_events'
    date: str = "2024-03-28"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name, base_table, date)
    if output_df is not None:
        output_df.write.mode("overwrite").insertInto(output_table_name)
