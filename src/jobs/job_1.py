from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str, year: int) -> str:
    query = f"""
    SELECT *
    FROM {output_table_name}
    WHERE quality_class = 'high' AND is_active = True AND current_year = {year}
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, year: int) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(output_table_name, year))

def main():
    output_table_name: str = "saisiddu201266140.actors"
    year: int = 2024
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("SelectActiveHighQualityActors")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, year)
    output_df.write.mode("overwrite").insertInto(output_table_name)

