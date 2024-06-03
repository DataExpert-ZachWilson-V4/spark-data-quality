from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    query = f"""
    with last_year as( 
        select * from {output_table_name}
        where current_year = 2007
    ), --get the previous year's data from your table
    this_year as (
        select * from bootcamp.actor_films
        where year = 2008
    ) -- get the latest year's data from source table
    --compare last year's data with this year's data to populate the fields. 
    --To get all the rows from last_year and this year, we use full outer join
    select 
        coalesce(ly.actor_id,ty.actor_id) as actor_id, -- using coalesce to avoid null values
        coalesce(ly.actor, ty.actor) as actor,
        case  
            when ty.year is null then ly.films
            when ty.year is not null and ly.films is null then ARRAY[
                ROW(
                    ty.film,
                    ty.votes,
                    ty.rating,
                    ty.film_id)
                ]
            when ty.year is not null and ly.films is not null then ARRAY[
                ROW(
                    ty.film,
                    ty.votes,
                    ty.rating,
                    ty.film_id)] || ly.films
        END as films, 
        -- check if latest_years's data is null then use previous year's data for films. If previous year's data is null and this year's data is available populate it for films. If both years data is available then use concat to append this year's data to be at first.
        case 
            when AVG(ty.rating) over (partition by ty.actor_id) <= 6 then 'bad'
            when AVG(ty.rating) over (partition by ty.actor_id) <= 7 then 'average'
            when AVG(ty.rating) over (partition by ty.actor_id) <= 8 then 'good'
            when AVG(ty.rating) over (partition by ty.actor_id) > 8 then 'star' 
        END as quality_class, -- using the rules provided for categorizing quality class
        ty.year is not null AS is_active, 
        coalesce(ty.year, ly.current_year+1) as current_year
    from last_year ly
    full outer join this_year ty 
    on ly.actor_id = ty.actor_id
    """
    return query

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "actors"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
