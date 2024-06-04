from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SQLContext

def query_2(input_table_name: str, output_table_name: str) -> str:
    query = f"""
                WITH hosts AS (
                    SELECT
                    w.host AS host,
                    NULL as host_activity_datelist,
                    CAST(w.event_time AS DATE) AS `date`
                    FROM 
                    {input_table_name} w
                    ),
                    yesterday AS (
                        SELECT
                        *
                        FROM
                        {output_table_name}
                        WHERE
                        `date`= DATE('2023-01-06')
                    ),
                    today AS (
                        SELECT
                        host,
                        `date`,
                        COUNT(1)
                        FROM
                        hosts
                        WHERE
                        `date` = DATE('2023-01-07')
                        GROUP BY
                        host,
                        `date`
                    )
                    SELECT
                    COALESCE(y.host, t.host) AS host,
                    CASE
                        WHEN y.host_activity_datelist IS NOT NULL THEN ARRAY(t.`date`) || y.host_activity_datelist
                        ELSE ARRAY(t.`date`)
                    END AS host_activity_datelist,
                    t.`date` as `date`
                    FROM
                    yesterday y
                    FULL OUTER JOIN today t ON y.host = t.host
    """
    return query

def job_2(spark_session: SparkSession, input_table_name: str, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  spark_session.sql(query_2(input_table_name,output_table_name)).show()
  return spark_session.sql(query_2(input_table_name,output_table_name))

def main():
    output_table_name: str = "hosts_cumulated"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
