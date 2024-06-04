import datetime
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


# The query from week2-Query6 to incrementally populate the hosts_cumulated table from the web_events table
def query_2(
    cumulated_table_name: str, daily_table_name: str, yesterday: str, today: str
) -> str:
    query = f"""
        WITH yesterday_cte AS (
            SELECT
            *
            FROM
                {cumulated_table_name}
            WHERE
            DATE = DATE('{yesterday}')
        ),
        today_cte AS (
            SELECT
            host,
            CAST(date_trunc('day', event_time) AS DATE) AS event_date,
            COUNT(1)
            FROM
                {daily_table_name}
            WHERE
            date_trunc('day', event_time) = DATE('{today}')
            GROUP BY
            host,
            CAST(date_trunc('day', event_time) AS DATE)
        )
        SELECT
        COALESCE(y.host, t.host) AS host,
        CASE
            WHEN y.host_activity_datelist IS NOT NULL THEN concat(array(t.event_date), y.host_activity_datelist)        
            ELSE array(t.event_date)
        END AS host_activity_datelist,
        DATE('{today}') AS DATE
        FROM
        yesterday_cte y
        FULL OUTER JOIN today_cte t ON y.host = t.host
    """
    return query


def job_2(
    spark_session: SparkSession,
    cumulated_dataframe: DataFrame,
    cumulated_table_name: str,
    daily_dataframe: DataFrame,
    daily_table_name: str,
    today: str,
) -> Optional[DataFrame]:
    date_format = "%Y-%m-%d"
    today = datetime.datetime.strptime(str(today), date_format).date()
    yesterday = today - datetime.timedelta(days=1)

    print(today)
    print(yesterday)

    cumulated_dataframe.createOrReplaceTempView(cumulated_table_name)
    daily_dataframe.createOrReplaceTempView(daily_table_name)

    return spark_session.sql(
        query_2(cumulated_table_name, daily_table_name, yesterday, today)
    )


def main():
    cumulated_table_name: str = "hosts_cumulated"
    daily_table_name: str = "web_events"

    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    spark_session.conf.set("spark.sql.session.timeZone", "UTC")
    cumulated_df = spark_session.table(cumulated_table_name)
    daily_df = spark_session.table(daily_table_name)
    today = str(datetime.date.today())

    output_df = job_2(
        spark_session,
        cumulated_df,
        cumulated_table_name,
        daily_df,
        daily_table_name,
        today,
    )

    output_df.write.mode("append").insertInto(cumulated_table_name)
