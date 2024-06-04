from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import (
    StructType, StructField, ArrayType, StringType, LongType
)

def int_to_binary(num):
    return bin(num)[2:] if num >= 0 else '-' + bin(num)[3:]

binary_udf = udf(int_to_binary, StringType())

def query_1(input_table_name: str) -> str:
    query = f"""
    WITH
    today AS (
        SELECT
            user_id, browser_type, dates_active, date
        FROM
            {input_table_name}
        WHERE
            date = DATE('2023-01-01')
    ),
    date_list_int AS (
        SELECT
            user_id, browser_type,
            CAST(
                SUM(
                    CASE
                        WHEN ARRAY_CONTAINS(dates_active, CAST(col AS STRING)) THEN POW(2, 30 - DATEDIFF(col, date))
                        ELSE 0
                    END
                ) AS BIGINT
            ) AS history_int
        FROM
            today
        CROSS JOIN EXPLODE(SEQUENCE(DATE('2023-01-01'), DATE('2023-01-07'))) AS col
        GROUP BY
            user_id, browser_type
    )
    SELECT *
    FROM
        date_list_int
    """
    return query

def job_1(spark_session: SparkSession, input_df: DataFrame, input_table_name: str) -> Optional[DataFrame]:
    input_df.createOrReplaceTempView(input_table_name)
    query_result = spark_session.sql(query_1(input_table_name))
    result_df = query_result.withColumn("history_in_binary", binary_udf(query_result.history_int))
    return result_df

def main():
    input_table_name: str = "user_devices_cumulated"
    output_table_name: str = "user_devices_cumulated_bitwise"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    input_df = job_1(spark_session, input_table_name)
    output_df = job_1(spark_session, input_df, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
