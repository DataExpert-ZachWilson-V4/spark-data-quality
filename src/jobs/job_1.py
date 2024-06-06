from pyspark.sql import SparkSession

def job_1(spark_session: SparkSession, output_table_name: str):
    query = f"""
    CREATE OR REPLACE TEMP VIEW {output_table_name} AS
    SELECT
        host,
        host_activity_datelist,
        date
    FROM Hosts
    GROUP BY host, host_activity_datelist, date
    """
    spark_session.sql(query)
    result_df = spark_session.sql(f"SELECT * FROM {output_table_name}")
    return result_df

