import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, DoubleType
from jobs.job_1 import job_1  # Adjust these imports based on your actual file and function names
from jobs.job_2 import job_2

class SparkJobTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Initialize SparkSession"""
        cls.spark = SparkSession.builder \
            .master("local[1]") \
            .appName("UnitTestsForSparkJobs") \
            .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession"""
        cls.spark.stop()

    def test_job_1(self):
        """Test case for job_1"""
        # Define schema and test data
        schema = StructType([
            StructField("actor_id", StringType(), True),
            StructField("actor", StringType(), True),
            StructField("films", ArrayType(StructType([
                StructField("film_name", StringType(), True),
                StructField("votes", IntegerType(), True),
                StructField("rating", DoubleType(), True),
                StructField("film_id", StringType(), True)
            ])), True),
            StructField("quality_class", StringType(), True),
            StructField("is_active", BooleanType(), True),
            StructField("current_year", IntegerType(), True)
        ])
        data = [
            ("1", "Actor One", [("Film A", 100, 8.5, "F1")], "high", True, 2024),
            ("2", "Actor Two", [("Film B", 200, 7.2, "F2")], "low", False, 2024)
        ]
        df = self.spark.createDataFrame(data, schema)
        df.createOrReplaceTempView("test_actors")

        # Execute job
        result_df = job_1(self.spark, "test_actors", 2024)

        # Check results
        self.assertEqual(result_df.count(), 1)
        self.assertEqual(result_df.first()["actor"], "Actor One")

    def test_job_2(self):
        """Test case for job_2"""
        # Define schema and test data
        schema = StructType([
            StructField("actor", StringType(), False),
            StructField("films", ArrayType(StructType([
                StructField("film_name", StringType(), False),
                StructField("votes", IntegerType(), False),
                StructField("rating", DoubleType(), False),
                StructField("film_id", StringType(), False)
            ])), False)
        ])
        data = [
            ("Actor One", [("Film A", 100, 8.5, "F1"), ("Film C", 150, 9.0, "F3")]),
            ("Actor Two", [("Film B", 200, 7.2, "F2")])
        ]
        df = self.spark.createDataFrame(data, schema)
        df.createOrReplaceTempView("test_actors")

        # Execute job
        result_df = job_2(self.spark, "test_actors")

        # Verify results
        self.assertEqual(result_df.count(), 2)
        self.assertEqual(result_df.filter("actor = 'Actor One'").select("number_of_films").collect()[0][0], 2)

if __name__ == '__main__':
    unittest.main()
