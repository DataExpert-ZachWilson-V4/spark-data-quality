from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    query = f"""
        -- Let's start by understanding our 2 tables.
        -- - web_events Table looks like this:
        -- user_id	      device_id	referrer	host	        url	  event_time
        -- 1967566579	-1138341683	null	www.eczachly.com	/	  2021-01-18 23:57:37.316 UTC
        -- 1272828233	-643696601	null	www.zachwilson.tech	/	  2021-01-18 00:10:52.986 UTC
        -- 694175222	1847648591	null	www.eczachly.com	/	  2021-01-18 00:15:29.251 UTC

        -- - devices Tables looks like this:
        -- device_id	 browser_type	            os_type	  device_type
        -- -2147042689	 Firefox	                Ubuntu	  Other
        -- -2146219609	 WhatsApp	                Other	  Spider
        -- -2145574618	 Chrome Mobile	            Android	  Generic Smartphone
        -- -2144707350	 Chrome Mobile WebView	    Android	  Samsung SM-G988B
        -- -2143813999	 Mobile Safari UI/WKWebView	iOS	      iPhone

        -- Using a CTE to perform necessary joins and aggregation
        WITH enriched_events AS (
            SELECT 
                w.user_id,
                d.browser_type,
                DATE(event_time) AS event_date
            FROM web_events w
            JOIN devices d ON w.device_id = d.device_id
        ),

        -- Aggregate new dates for each user and browser combination
        new_activity AS (
            SELECT 
                user_id,
                browser_type,
                ARRAY_AGG(DISTINCT event_date) AS dates_active
            FROM enriched_events
            GROUP BY user_id, browser_type
        )

        -- Simple insert for the initial population
        SELECT 
            user_id,
            browser_type,
            dates_active,
            CURRENT_DATE
        FROM new_activity
    """
    return query

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "<output table name here>"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)
