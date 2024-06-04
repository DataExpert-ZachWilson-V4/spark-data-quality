from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = \
        f"""
            with
                yesterday as (
                    select * from {{output_table_name}} where date = DATE('2024-05-19')
                ),
                today as (
                    select
                        web_e.user_id,
                        d.browser_type,
                        CAST(date_trunc('day', web_e.event_time) as DATE) as event_date,
                        COUNT(1)
                    from bootcamp.web_events as web_e left join bootcamp.devices as d
                        on d.device_id = web_e.device_id
                    where date_trunc('day', web_e.event_time) = DATE('2024-05-20')
                    group by
                        web_e.user_id,
                        d.browser_type,
                        CAST(date_trunc('day', web_e.event_time) as DATE)
                )
            select
                COALESCE(y.user_id, t.user_id) as user_id,
                COALESCE(y.browser_type, t.browser_type) as browser_type,
                case
                    when y.dates_active is not NULL
                        then CONCAT(array[t.event_date], y.dates_active)
                    else
                        array[t.event_date]
                end as dates_active,
                DATE('2024-05-20') as date
            from yesterday as y full outer join today as t 
                on y.user_id = t.user_id and y.browser_type = t.browser_type
    """
    return query

def job_2(
        spark_session: SparkSession, output_table_name: str
) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "shabab.user_devices_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
