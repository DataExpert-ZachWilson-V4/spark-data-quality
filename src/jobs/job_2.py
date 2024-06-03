from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

CURRENT_YEAR = 1939


def query_2(
    source_table_name: str, cumulative_table_name: str, CURRENT_YEAR: int
) -> str:
    query = f"""
        with actors_last_yr as (
        select
            *
        from
           {cumulative_table_name}
       where
            CURRENT_YEAR = {CURRENT_YEAR}
        ),
        actors_this_yr as (
        select
            actor,
            actor_id,
            COLLECT_LIST(
            struct(
                film,
                film_id,
                votes,
                rating,
                year
            )
            ) AS films,
            AVG(rating) AS avg_rating,
            year
        FROM
            {source_table_name}
        WHERE
            rating is not null
            and year = {CURRENT_YEAR}+1
        GROUP BY
            actor,
            actor_id,
            year
        )
        select
        coalesce(aly.actor, aty.actor) as actor,
        coalesce(aly.actor_id, aty.actor_id) as actor_id,
        case
            when aty.films is null then aly.films
            when aty.films is not null
            and aly.films is null then aty.films
            when aty.films is not null
            and aly.films is not null then aty.films || aly.films
        end as films,
        CASE
            WHEN avg_rating > 8 THEN 'star'
            WHEN avg_rating > 7 THEN 'good'
            WHEN avg_rating > 6 THEN 'average'
            ELSE 'bad'
        END AS quality_class,
        aty.year is not null as is_active,
        coalesce(aty.year, aly.CURRENT_YEAR + 1) as CURRENT_YEAR
        from
        actors_last_yr aly FULL
        OUTER JOIN actors_this_yr aty ON aly.actor_id = aty.actor_id
    """
    return query


def job_2(
    spark: SparkSession,
    source_table_name,
    cumulative_table_name: str,
    CURRENT_YEAR: int,
) -> Optional[DataFrame]:
    ingest_df = spark.table(source_table_name)
    output_df = spark.table(cumulative_table_name)
    ingest_df.createGlobalTempView(source_table_name)
    output_df.createOrReplaceTempView(cumulative_table_name)
    return spark.sql(query_2(source_table_name, cumulative_table_name, CURRENT_YEAR))


def main():
    source_table_name = "actor_films"
    cumulative_table_name: str = "actors"
    spark: SparkSession = (
        SparkSession.builder.master("local").appName("job_2").getOrCreate()
    )
    output_df = job_2(spark, source_table_name, cumulative_table_name, CURRENT_YEAR)
    output_df.write.mode("overwrite").insertInto(cumulative_table_name)
