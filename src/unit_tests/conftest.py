# I had to include the conftest file here because it wasn't getting
# picked up by pytest
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("PySpark Test") \
        .getOrCreate()

    yield spark
    spark.stop()