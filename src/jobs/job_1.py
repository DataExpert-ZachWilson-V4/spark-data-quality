from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

#
def query_1(input_table_name: str) -> str:
    return f"""
            WITH
        actorfilms AS ( -- get aggregated data for avg rating for each actor per year
        SELECT
        actor
        ,actor_id 
        ,AVG(rating) as avg_rating
        ,year as current_year
        FROM 
        {input_table_name}
        WHERE
        year = 1917
        group by actor,actor_id,year
    )
    SELECT 
    actor,
    actor_id,
    CASE WHEN avg_rating IS NOT NULL THEN
        CASE WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating > 7 THEN 'good'
            WHEN avg_rating > 6 THEN 'average'
            WHEN avg_rating <= 6 THEN 'bad' END
        ELSE NULL END as quality_class,
    current_year
    FROM
    actorfilms
    """
    return  query


def job_1(spark_session: SparkSession, input_df: DataFrame, input_table_name: str) -> Optional[DataFrame]:
    input_df.createOrReplaceTempView(input_table_name)
    return spark_session.sql(query_1(input_table_name))

def main():
    input_table_name: str = "actor_films"
    output_table_name: str = "actor_films_agg"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    input_df = spark_session.table(input_table_name)
    output_df = job_1(spark_session, input_df, input_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)