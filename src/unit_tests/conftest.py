import pytest
from pyspark.sql import SparkSession

def spark_session_factory(app_name: str = "chispa") -> SparkSession:
  return (
      SparkSession.builder
      .master("local")
      .appName(app_name)
      .getOrCreate()
  )

@pytest.fixture(scope='session')
def spark_session():
    return spark_session_factory()
