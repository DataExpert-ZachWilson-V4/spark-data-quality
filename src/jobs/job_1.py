from typing import Optional
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


def query_1(
        etn: str,
        dtn: str,
        otn: str,
        cd: str) -> str:
    dt = datetime.strptime(cd, "%Y-%m-%d")
    pd = dt - timedelta(days=1)
    pd_s = pd.strftime("%Y-%m-%d")


    query = f"""
    WITH yesterday AS (
        SELECT *
        FROM 
            {otn}
        WHERE 
            date = DATE('{pd_s}')
    ),
    today AS (
        SELECT
            we.user_id,
            d.browser_type,
            DATE('{cd}') as todays_date,
            ARRAY_AGG(DATE(we.event_time)) AS event_dates
        FROM
            {dtn} d
            JOIN {etn} we ON d.device_id = we.device_id
        WHERE DATE(we.event_time) = DATE('{cd}')
        GROUP BY we.user_id, d.browser_type
    )
    SELECT
        COALESCE(yesterday.user_id, today.user_id) AS user_id,
        COALESCE(yesterday.browser_type, today.browser_type) AS browser_type,
        CASE 
            WHEN yesterday.user_id IS NULL AND today.todays_date IS NOT NULL THEN ARRAY(today.todays_date)
            WHEN yesterday.dates_active IS NOT NULL AND today.todays_date IS NOT NULL THEN ARRAY(today.todays_date) || yesterday.dates_active
            WHEN yesterday.dates_active IS NOT NULL AND today.todays_date IS NULL THEN yesterday.dates_active
            ELSE NULL
        END AS dates_active,
        COALESCE(today.todays_date, yesterday.date + INTERVAL 1 DAY) AS date
    FROM
        yesterday
        FULL OUTER JOIN today
        ON yesterday.user_id = today.user_id
        AND yesterday.browser_type = today.browser_type
    """
    return query


def job_1(
        spark_session: SparkSession,
        etn: str,
        dtn: str,
        otn: str,
        cd: str) -> Optional[DataFrame]:

    odf = spark_session.table(otn)
    odf.createOrReplaceTempView(otn)

    rdf = spark_session.sql(
        query_1(
            etn,
            dtn,
            otn,
            cd
        )
    )
    return rdf


def main():
    otn: str = "user_devices_cumulated"

    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )

    odf = job_1(
        spark_session,
        "web_events",
        "user_devices",
        "user_devices_cumulated",
        "2023-01-14"
    )

    if odf is not None:
        odf.write.mode("overwrite").insertInto(otn)
    else:
        print("No DataFrame to write to the output table.")


if __name__ == "__main__":
    main()
