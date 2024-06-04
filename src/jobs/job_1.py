from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(output_table_name: str) -> str:
    query = f"""
WITH last_year AS (
  SELECT * from {output_table_name}
  WHERE current_year = 2000
),
this_year AS (
  SELECT 
    actor,
    actor_id,
    COLLECT_LIST(ARRAY(
        film,
        votes,
        rating,
        film_id,
        year
        )) as films,
   SUM(votes*rating)/SUM(votes) as avg_rating, --logic to caluculate the avg_rating
   year    
  FROM actor_films
  WHERE year = 2001 and votes is not null and votes <> 0
  group by actor, actor_id, year
)

SELECT 
  COALESCE(ly.actor, ty.actor) as actor,
  COALESCE(ly.actor_id, ty.actor_id) as actor_id,
 CASE -- case statement for calculating the films array
      WHEN ty.films IS NULL THEN ly.films
      WHEN ly.films IS NULL THEN ty.films
      WHEN ty.films IS NOT NULL AND ly.films IS NOT NULL 
        THEN (ty.films || ly.films)
END AS films,
 CASE -- case statement to calculate the quality_class based on average rating
     WHEN ty.avg_rating is NOT NULL AND ty.avg_rating > 8 THEN 'star'
     WHEN ty.avg_rating is NOT NULL AND ty.avg_rating > 7 AND ty.avg_rating <= 8 THEN 'good'
     WHEN ty.avg_rating is NOT NULL AND ty.avg_rating > 6 AND ty.avg_rating <= 7 THEN 'average'
     WHEN ty.avg_rating is NOT NULL AND ty.avg_rating <= 6 THEN 'bad'
  END AS quality_class, 
  CASE 
    WHEN ty.year is NOT NULL then TRUE
    ELSE FALSE
  END as is_active,
  COALESCE(ty.year, ly.current_year+1) as current_year
FROM last_year ly 
FULL OUTER JOIN this_year ty
ON ly.actor_id=ty.actor_id

   """
    return query


def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
    output_df = spark_session.table(output_table_name)
    output_df.createOrReplaceTempView(output_table_name)
    return spark_session.sql(query_1(output_table_name))


def main():
    output_table_name: str = "actors"
    spark_session: SparkSession = (
        SparkSession.builder.master("local").appName("job_1").getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
