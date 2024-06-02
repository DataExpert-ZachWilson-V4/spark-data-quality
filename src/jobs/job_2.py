from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import size

def query_2(output_table_name: str) -> str:
    query = f"""
    SELECT actor, size(films) as number_of_films
    FROM {output_table_name}
    GROUP BY actor
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "saisiddu201266140.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("CountFilmsByActor")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.show()  # For testing purposes, showing the results. Replace with write command for production use.

