import sys
from datetime import datetime
from pyspark.sql import SparkSession
from typing import Optional
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    WITH
    last_film AS (
        SELECT
        *
        FROM
        {output_table_name}
        WHERE
        current_year = 2008
    ),
    current_film AS (
        SELECT
        actor,
        actor_id,
        ARRAY_AGG(CAST(ROW(year, film, votes, rating, film_id) AS 
                        ROW(year INTEGER, film VARCHAR, votes INTEGER, rating DOUBLE, film_id VARCHAR))) AS films,
        year
        FROM
        bootcamp.actor_films
        WHERE
        YEAR = 2009
        GROUP BY actor,actor_id, year
    )

    SELECT
    COALESCE(lf.actor, cf.actor) AS actor,
    COALESCE(lf.actor_id, cf.actor_id) AS actor_id,
    CASE
        WHEN cf.year IS NULL THEN lf.films
        WHEN cf.year IS NOT NULL AND lf.films IS NULL THEN cf.films
        WHEN cf.year IS NOT NULL AND lf.films IS NOT NULL THEN cf.films || lf.films
    END AS films,
    CASE
        WHEN cf.year IS NULL THEN lf.quality_class
        WHEN cf.year IS NOT NULL THEN REDUCE( cf.films, CAST(ROW(0.0, 0) AS 
        ROW(sum DOUBLE, count INTEGER)), 
            (s, r) -> CAST(ROW(r.rating + s.sum, s.count + 1) AS ROW(sum DOUBLE, count INTEGER)),
        s -> CASE WHEN s.sum / s.count > 8 THEN 'star' 
        WHEN s.sum/s.count > 7 and s.sum/s.count <= 8 THEN 'good'
        WHEN s.sum/s.count > 6 and s.sum/s.count <= 7 THEN 'average'
        else 'bad' 
        END
    ) 
    END AS quality_class,
    cf.year IS NOT NULL AS is_active,
    COALESCE(cf.year, lf.current_year + 1) AS current_year
    FROM
    last_film lf
    FULL OUTER JOIN current_film cf ON lf.actor_id = cf.actor_id
        """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "ykshon52797255.actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
