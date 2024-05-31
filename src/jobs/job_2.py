from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(input_table_name: str) -> str:
    query = f""" -- Homework 2 - Query 4
    WITH today AS (
        SELECT *
        FROM {input_table_name}
        WHERE date = '2023-01-04'
        -- We take the data from the last day cumulated. 
        -- Because it will have the most up-to-date information.
        -- And all the data we need to build a binary history.
    ),
    date_list_int AS (
        SELECT
            user_id,
            browser_type,
            CAST(
                SUM(
                    CASE
                        WHEN array_contains(dates_active, sequence_date) THEN POW(2, 31 - DATEDIFF(date, sequence_date))
                        ELSE 0
                    END
                ) AS BIGINT
            ) AS history_int
            -- Sums the powers of 2 for each date in the dates_active array.
            -- (We did 31 and not 32 bits, because one bit is the sign bit).
            -- This will get us a binary representation of the dates_active array.
            -- 1 if the date is present, 0 if it is not. From left to right most oldest to recent date.
        FROM today
        LATERAL VIEW explode(sequence(to_date('2023-01-01'), to_date('2023-01-04'))) AS sequence_date 
        -- LATERAL VIEW explode replaces TRINO's CROSS JOIN UNNEST.
        GROUP BY user_id, browser_type
    )
    SELECT
        *,
        bin(history_int) AS history_in_binary
        -- Converts the integer to its binary string representation.
    FROM date_list_int
    """
    return query

def job_2(spark_session: SparkSession, in_dataframe: DataFrame) -> Optional[DataFrame]:
    input_table = 'user_devices_cumulated'
    in_dataframe.createOrReplaceTempView(input_table)
    return spark_session.sql(query_2(input_table))

def main():
    output_table_name: str = "hw2_query4_spark"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, spark_session.table("jsgomez14.user_devices_cumulated"))
    output_df.write.mode("overwrite").insertInto(output_table_name)
