from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
   INSERT INTO
deeptianievarghese22866.actors_history_scd  --incremental load of scd table from actors table
WITH
  last_year_scd AS (   --read scd table for previous years
    SELECT
      *
    FROM
actors_history_scd
    WHERE
      current_year = 2021
  ),
  this_year_scd AS (   --read actor table for most recent year
    SELECT
      *
    FROM
     actors
    WHERE
      current_year = 2022
  ),
  combined AS (   --full outer join both scd table with historic data and actor table wth most recent data
    SELECT
      COALESCE(l.actor, t.actor) AS actor,   --merge common columns
      COALESCE(l.quality_class, t.quality_class) AS quality_class,
      COALESCE(l.start_date, t.current_year) AS start_date,
      COALESCE(l.end_date, t.current_year) AS end_date,
      CASE    --case statement to track state change from previous year to current year
        WHEN l.is_active <> t.is_active OR l.quality_class <> t.quality_class THEN 1
        WHEN l.is_active = t.is_active AND l.quality_class = t.quality_class THEN 0
      END AS did_change,
      l.is_active AS is_active_last_year,
      t.is_active AS is_active_this_year,
      l.quality_class AS quality_class_last_year,
      t.quality_class AS quality_class_this_year,
coalesce(l.current_year+1,t.current_year) as current_year
    FROM
      last_year_scd l
      FULL OUTER JOIN this_year_scd t ON l.actor = t.actor  --full outer join scd table with actor table on actor and year
      AND l.end_date + 1 = t.current_year
  ),
  changes AS (   --track state changes and populate array accordingly with active/inactive state, start and end dates
    SELECT
     actor,
     quality_class,
      current_year,
      CASE
        WHEN did_change = 0 THEN ARRAY[  --no state change from last year to current year-update end date to current year
          CAST(
            ROW(
              quality_class,
              is_active_last_year,
              start_date,
              end_date + 1
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change = 1 THEN ARRAY[ --there is state change from last year to current year-add two records - for last year and current year
          CAST(
            ROW(quality_class,is_active_last_year, start_date, end_date) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          ),
          CAST(
            ROW(
              quality_class,
              is_active_this_year,
              current_year,
              current_year
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
        WHEN did_change IS NULL THEN ARRAY[ --state change is null-actor has no historic records - add current year record to array
          CAST(
            ROW(
              COALESCE(quality_class_last_year, quality_class_this_year),
              COALESCE(is_active_last_year, is_active_this_year),
              start_date,
              end_date
            ) AS ROW(
              quality_class varchar,
              is_active boolean,
              start_date integer,
              end_date integer
            )
          )
        ]
      END AS change_array
    FROM
      combined
  )
SELECT
  actor,
  arr.quality_class,
  arr.is_active,  --explode change array to populate scd table
  arr.start_date,
  arr.end_date,
  current_year
FROM
  changes
  CROSS JOIN UNNEST (change_array) AS arr 
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "deeptianievarghese22866.actors_history_scd"  
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
