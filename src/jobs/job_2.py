from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    """
    Interesting Note: array[] isn't supported in pyspark, had to use array()
    """
    query = \
        f"""
            with
                yesterday as (
                    select * from user_devices_cumulated where date = DATE('2022-01-01')
                ),
                today as (
                    select
                        web_e.user_id,
                        d.browser_type,
                        CAST(date_trunc('day', web_e.event_time) as DATE) as event_date,
                        COUNT(1)
                    from {output_table_name} as web_e left join devices as d
                        on d.device_id = web_e.device_id
                    where date_trunc('day', web_e.event_time) = DATE('2022-01-02')
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
                        then array(t.event_date) || y.dates_active
                    else
                         array(t.event_date)
                end as dates_active,
                DATE('2022-01-02') as date
            from yesterday y full outer join today t 
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
    output_table_name: str = "web_events"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
