from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(output_table_name: str) -> str:
    query = f"""
    WITH yesterday AS (
    SELECT * FROM {output_table_name}
    WHERE date = DATE('2023-01-05')
    ),
    today AS (
    SELECT 
        host, 
        CAST(date_trunc('day', event_time) as DATE) as event_date, 
        COUNT(1) 
    FROM web_events 
    WHERE date_trunc('day', event_time) = DATE('2023-01-06')
    GROUP BY host, CAST(date_trunc('day', event_time) AS DATE)
    )

    SELECT
    COALESCE(y.host, t.host) as host,
    CASE when y.host_activity_datelist is NULL THEN ARRAY(t.event_date) --combine date activity from yesterday and today
    ELSE ARRAY(t.event_date) || y.host_activity_datelist
    END as host_activity_datelist,
    DATE('2023-01-06') as date
    from yesterday y
    full outer join today t
    on y.host = t.host
    """
    return query


def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name))


def main():
    output_table_name: str = "hosts_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
