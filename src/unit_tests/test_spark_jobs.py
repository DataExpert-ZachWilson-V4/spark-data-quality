import pytest
from datetime import date, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, date_trunc, lit, when, array
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Ensure Spark is available in the test environment
from pyspark.testing.utils import ReusedPySparkTestCase

class TestSparkJobs(ReusedPySparkTestCase):
    @pytest.fixture(scope='module', autouse=True)
    def spark(self):
        return SparkSession.builder.master("local").appName("unittests").getOrCreate()

    def test_job_1(self, spark):
        # Create sample data for web_events and devices
        web_events_schema = StructType([
            StructField("user_id", IntegerType()),
            StructField("device_id", IntegerType()),
            StructField("event_time", DateType())
        ])
        devices_schema = StructType([
            StructField("device_id", IntegerType()),
            StructField("browser_type", StringType())
        ])

        web_events_data = [
            (1, 1, date(2021, 1, 1)),
            (1, 1, date(2021, 1, 2)),
            (1, 2, date(2021, 1, 3)),
            (2, 2, date(2021, 1, 1)),
            (2, 2, date(2021, 1, 2)),
            (2, 1, date(2021, 1, 3))
        ]
        devices_data = [
            (1, "Chrome"),
            (2, "Safari")
        ]

        web_events_df = spark.createDataFrame(web_events_data, schema=web_events_schema)
        devices_df = spark.createDataFrame(devices_data, schema=devices_schema)

        web_events_df.createOrReplaceTempView("web_events")
        devices_df.createOrReplaceTempView("devices")

        # Simulate job_1 processing logic
        current_date = date(2021, 1, 3)
        yesterday_date = current_date - timedelta(days=1)

        result_df = spark.sql(
            f"""
            SELECT e.user_id, d.browser_type,
                   date_trunc('day', e.event_time) AS event_date,
                   COUNT(1) AS event_count
            FROM web_events e
            LEFT JOIN devices d ON e.device_id = d.device_id
            WHERE date_trunc('day', e.event_time) = DATE('{current_date}')
            GROUP BY e.user_id, d.browser_type, date_trunc('day', e.event_time)
            """
        )

        expected_data = [
            (1, "Safari", date(2021, 1, 3), 1),
            (2, "Chrome", date(2021, 1, 3), 1)
        ]
        expected_schema = StructType([
            StructField("user_id", IntegerType()),
            StructField("browser_type", StringType()),
            StructField("event_date", DateType()),
            StructField("event_count", IntegerType())
        ])
        expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

    def test_job_2(self, spark):
        # Create sample data for web_events
        web_events_schema = StructType([
            StructField("user_id", IntegerType()),
            StructField("device_id", IntegerType()),
            StructField("host", StringType()),
            StructField("event_time", DateType())
        ])

        web_events_data = [
            (1, 1, "www.eczachly.com", date(2021, 1, 2)),
            (2, 1, "www.zachwilson.tech", date(2021, 1, 2))
        ]

        web_events_df = spark.createDataFrame(web_events_data, schema=web_events_schema)
        web_events_df.createOrReplaceTempView("web_events")

        # Simulate job_2 processing logic
        current_date = date(2021, 1, 2)

        result_df = spark.sql(
            f"""
            SELECT host, date_trunc('day', event_time) AS event_date, COUNT(1) AS event_count
            FROM web_events
            WHERE date_trunc('day', event_time) = DATE('{current_date}')
            GROUP BY host, date_trunc('day', event_time)
            """
        )

        expected_data = [
            ("www.eczachly.com", date(2021, 1, 2), 1),
            ("www.zachwilson.tech", date(2021, 1, 2), 1)
        ]
        expected_schema = StructType([
            StructField("host", StringType()),
            StructField("event_date", DateType()),
            StructField("event_count", IntegerType())
        ])
        expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

        self.assertEqual(sorted(result_df.collect()), sorted(expected_df.collect()))

# To run tests using pytest in a script that understands Spark
if __name__ == "__main__":
    pytest.main([__file__])
