from datetime import datetime, timedelta
from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_2(output_table_name, start_date) -> str:
    day_after_start_date = (
        datetime.strptime(start_date, "%Y-%m-%d") + timedelta(days=1)
    ).strftime("%Y-%m-%d")
    query = f"""
    with
        prev as (
            select
                *
            from
                {output_table_name}
            where
                date = date('{start_date}')
        ),
        curr as (
            select
                host,
                date(event_time) as date
            from
                web_events
            where
              date(event_time) = date('{day_after_start_date}')
            group by -- since want one record per host/day
                1,
                2
        )
    select
        coalesce(p.host, c.host) as host,
        case
            when p.host_activity_datelist is null then array(c.date) -- case when not previously in cumulative table then start array of dates with current date
            when c.date is null then p.host_activity_datelist -- case when host not occuring on current date then array of dates is same as previous host_activity_datelist
            else array(c.date) || p.host_activity_datelist -- case when previously in cumulative tabel and host occuring on current date then add current date as first element in previous host_activity_datelist
        end as host_activity_datelist,
        coalesce(p.date + interval '1' day, c.date) as date
    from
        prev p
        full outer join curr c on p.host = c.host
        and p.date + interval '1' day = c.date
    """
    return query


def job_2(
    spark_session: SparkSession,
    web_events_df: DataFrame,
    hosts_cumulated_df: DataFrame,
    output_table_name: str,
    start_date: str,
) -> Optional[DataFrame]:
    web_events_df.createOrReplaceTempView("web_events")
    hosts_cumulated_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name, start_date))


def main():
    output_table_name: str = "hosts_cumulated"
    start_date = "2021-01-03"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(
        spark_session,
        spark_session.table("web_events"),
        spark_session.table(output_table_name),
        output_table_name,
        start_date,
    )
    output_df.write.mode("overwrite").insertInto(output_table_name)
