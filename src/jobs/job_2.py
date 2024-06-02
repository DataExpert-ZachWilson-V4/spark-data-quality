from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

current_year = 2003


def query_2(output_table_name: str, current_year: int) -> str:
    query = f"""
        WITH

            last_year AS (
                SELECT
                    *
                FROM
                    {output_table_name}
                WHERE
                    current_year = {current_year}
            ),

            this_year AS (
                SELECT
                    actor,
                    actor_id,
                    -- Aggregating all film details into an array for each actor
                    array_agg (STRUCT(year, film, votes, rating, film_id)) AS films,
                    -- Determining the 'quality_class' based on the average film rating
                    CASE
                        WHEN AVG(rating) > 8 THEN 'star'
                        WHEN AVG(rating) > 7 THEN 'good'
                        WHEN AVG(rating) > 6 THEN 'average'
                        ELSE 'bad'
                    END AS quality_class,
                    year
                FROM
                    actor_films
                WHERE
                    year = {current_year+1}
                    -- Grouping the results by actor, actor_id, and year for aggregation
                GROUP BY
                    actor,
                    actor_id,
                    year
            )

        SELECT
            COALESCE(ly.actor, ty.actor) AS actor,
            COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
            CASE
                WHEN ly.films IS NULL THEN ty.films
                WHEN ty.films IS NOT NULL THEN ly.films || ty.films
                ELSE ly.films
            END AS films,
            COALESCE(ty.quality_class, ly.quality_class) AS quality_class,
            COALESCE(ty.actor_id, ly.actor_id) IS NOT NULL AS is_active,
            COALESCE(ty.year, ly.current_year + 1) AS current_year
        FROM
            last_year ly
            FULL OUTER JOIN this_year ty ON ty.actor_id = ly.actor_id
            AND ly.current_year = ty.year - 1
    """
    return query


def job_2(
    spark: SparkSession, output_table_name: str, current_year: int
) -> Optional[DataFrame]:
    output_df = spark.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark.sql(query_2(output_table_name, current_year))


def main():
    output_table_name: str = "actors"
    spark: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(spark, output_table_name, current_year)
    output_df.write.mode("overwrite").insertInto(output_table_name)
