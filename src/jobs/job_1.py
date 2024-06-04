from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str, current_date: str) -> str:
    query = f"""
        with yesterday as (
            select *
            from {output_table_name}
            where date = date('{current_date}') - interval '1' day
        ),

        today as (
            select 
                user_id, 
                CAST(date_trunc('day', event_time) as date) as event_date, 
                count(*) 
            from bootcamp.web_events
            where date_trunc('day', event_time) = date('{current_date}')
            group by 1, 2
        )

        select 
            coalesce(y.user_id, t.user_id) as user_id,
            case 
                when y.dates_active is not null 
                    then array[t.event_date] || y.dates_active
                else array[t.event_date]
            end as dates_active,
            date('{current_date}') as date
        from yesterday y 
        full outer join today t
            on y.user_id = t.user_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, current_date: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name, current_date))

def main():
    output_table_name: str = "dennisgera.web_users_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, "2023-01-01")
    output_df.write.mode("overwrite").insertInto(output_table_name)
