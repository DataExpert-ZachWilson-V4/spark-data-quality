from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    query = f"""
        WITH
            yesterday AS (
                -- select all columns from 'user_devices_cumulated' table for the date '2023-01-01'
                SELECT
                    *
                FROM
                    {output_table_name}
                WHERE
                    date = DATE ('2023-01-01')
            ),
            today AS (
                -- select user_id and browser_type, and aggregate distinct active dates for each user and browser type
                SELECT
                    we.user_id,
                    d.browser_type,
                    array_agg (
                        DISTINCT CAST(date_trunc ('day', we.event_time) AS DATE)
                    ) AS dates_active,
                    count(1) as event_count
                FROM
                    devices d
                    -- left join the 'devices' table with the 'web_events' table on device_id
                    LEFT JOIN web_events we ON we.device_id = d.device_id
                WHERE
                    -- filter events to include only those from '2023-01-02'
                    date_trunc ('day', we.event_time) = DATE ('2023-01-02')
                    -- Group by user_id and browser_type
                GROUP BY
                    we.user_id,
                    d.browser_type
            )
            -- select and combine data from 'yesterday' and 'today' CTEs
        SELECT
            -- use COALESCE to handle cases where either 'yesterday' or 'today' has a missing user_id or browser_type
            COALESCE(y.user_id, t.user_id) AS user_id,
            COALESCE(y.browser_type, t.browser_type) AS browser_type,
            -- combine active dates from 'yesterday' and 'today'
            CASE
                WHEN y.dates_active IS NOT NULL THEN t.dates_active || y.dates_active
                ELSE t.dates_active
            END AS dates_active,
            -- set the date for the inserted records to '2023-01-02'
            DATE ('2023-01-02') AS date
        FROM
            -- perform a full outer join on 'user_id' between 'yesterday' and 'today' CTEs
            yesterday y
            FULL OUTER JOIN today t ON y.user_id = t.user_id
    """
    return query


def job_1(spark: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark.sql(query_1(output_table_name))


def main():
    output_table_name: str = "devices_cumulated"
    spark: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_1(spark, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
