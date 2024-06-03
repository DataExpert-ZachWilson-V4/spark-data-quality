from typing import Optional
from pyspark.sql import SparkSession, DataFrame, Window
import pyspark.sql.functions as F


def query_2(otn: str) -> str:
    query = f"""
    WITH lagged AS (
        SELECT *, LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
               LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
        FROM {otn}
    ),
    streaked AS (
        SELECT *,
               SUM(
                   CASE
                       WHEN quality_class <> previous_quality_class THEN 1
                       WHEN is_active <> previous_is_active THEN 1
                       ELSE 0
                   END
               ) OVER (PARTITION BY actor_id ORDER BY current_year) AS streak_identifier
        FROM lagged
    ),
    cy AS (SELECT MAX(current_year) AS max_current_year FROM {otn})
    SELECT actor, actor_id, quality_class, is_active,
        DATE(CONCAT(CAST(MIN(current_year) AS STRING), '-01-01')) AS start_date,
        DATE(CONCAT(CAST(MAX(current_year) AS STRING), '-12-31')) AS end_date,
        MAX(cy.max_current_year) AS current_year
    FROM streaked CROSS JOIN cy GROUP BY actor, actor_id, quality_class, is_active, streak_identifier
    """
    return query


def job_2(spark_session: SparkSession, otn: str) -> Optional[DataFrame]:
    df = spark_session.table(otn)
    ws = Window.partitionBy("actor_id").orderBy("year")
    df = df.withColumn("quality_class", F.when(F.col("rating") >= 8.0, "star")
                       .when(F.col("rating") >= 7.0, "good")
                       .otherwise("average"))
    df = df.withColumn("is_active", F.lit(True))
    ldf = df.withColumn("previous_quality_class", F.lag("quality_class").over(ws)) \
        .withColumn("previous_is_active", F.lag("is_active").over(ws))
    sdf = ldf.withColumn("streak_identifier", F.sum(
        F.when(
            (F.col("quality_class") != F.col("previous_quality_class")) |
            (F.col("is_active") != F.col("previous_is_active")), 1
        ).otherwise(0)
    ).over(ws))
    max_current_year = df.agg(F.max("year").alias("max_current_year")).collect()[0]["max_current_year"]
    rdf = sdf.groupBy("actor", "actor_id", "quality_class", "is_active", "streak_identifier") \
        .agg(
            F.expr("DATE(CONCAT(CAST(MIN(year) AS STRING), '-01-01'))").alias("start_date"),
            F.expr("DATE(CONCAT(CAST(MAX(year) AS STRING), '-12-31'))").alias("end_date"),
            F.collect_list(F.struct("film_id", "film", "year", "votes", "rating")).alias("films")
        ).withColumn("current_year", F.lit(max_current_year))
    rdf = rdf.select("actor_id","actor", "films","quality_class","is_active","current_year")

    rdf = rdf.withColumn("films", F.expr("""
        transform(films, x -> array(x.film_id, x.film, cast(x.year as string), cast(x.votes as string), cast(x.rating as string)))
    """))

    rdf.createOrReplaceTempView(otn)
    return rdf

def main():
    otn: str = "actors_history"
    spark_session = SparkSession.builder.master("local").appName("job_2").getOrCreate()
    output_df = job_2(spark_session, otn)
    if output_df:
        output_df.write.mode("overwrite").insertInto(otn)

if __name__ == "__main__":
    main()
