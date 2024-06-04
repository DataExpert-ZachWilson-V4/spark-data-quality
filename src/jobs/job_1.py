from typing import Optional
from pyspark.sql import SparkSession # type: ignore
from pyspark.sql.dataframe import DataFrame # type: ignore

def query_1(output_table_name: str, current_year: int) -> str:
    query = f"""
    WITH last_year AS (
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
            actor_iD,
            ARRAY_AGG(ROW(film, votes, rating, film_id)) AS films,
            CASE
                WHEN avg_rating > 8 THEN 'star'
                WHEN avg_rating > 7 AND avg_rating <= 8 THEN 'good'
                WHEN avg_rating > 6 AND avg_rating <= 7 THEN 'average'
                ELSE 'bad'
            END AS quality_class,
            actor_id IS NOT NULL AS is_active,
            year AS current_year
        FROM
            actor_films
        WHERE
            YEAR = {current_year + 1}
        GROUP BY
            actor,
            actor_id,
            YEAR
    )
    SELECT
        COALESCE(ly.actor, ty.actor) AS actor,
        COALESCE(ly.actor_id, ty.actor_id) AS actor_id,
        CASE
            WHEN ty.films IS NULL THEN ly.films
            WHEN ty.films IS NOT NULL AND ly.films IS NULL THEN ty.films
            WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL THEN ty.films || ly.films
        END AS films,
        COALESCE(ly.quality_class, ty.quality_class) AS quality_class,
        ty.year IS NOT NULL AS is_active,
        COALESCE(ty.year, ly.current_year + 1) AS current_year
    FROM
        last_year ly
    FULL OUTER JOIN this_year ty
    ON
        ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str, current_year: int) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(output_table_name,current_year))

def main():
    output_table_name: str = "actors"
    current_year = 1950
    spark_session: SparkSession = (SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name, current_year)
    output_df.write.mode("overwrite").insertInto(output_table_name)

if __name__ == "__main__":
    main()  