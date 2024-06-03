from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(input_table_name: str) -> str:
    query = f"""WITH today AS (
        SELECT *
        FROM {input_table_name}
        WHERE date = '2023-01-04'
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
        FROM today
        LATERAL VIEW explode(sequence(to_date('2023-01-01'), to_date('2023-01-04'))) AS sequence_date 
        GROUP BY user_id, browser_type
    )
    SELECT
        *,
        bin(history_int) AS history_in_binary
    FROM date_list_int
    """
    return query

def job_2(spark_session: SparkSession, in_dataframe: DataFrame) -> Optional[DataFrame]:
    input_table = 'user_devices_cumulated'
    in_dataframe.createOrReplaceTempView(input_table)
    return spark_session.sql(query_2(input_table))

def main():
    output_table_name: str = "hw4_query_spark"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, spark_session.table("sagararora492.user_devices_cumulated"))
    output_df.write.mode("overwrite").insertInto(output_table_name)