import pytest
from pyspark.sql import SparkSession

def spark_session_factory() -> SparkSession:
  return (
      SparkSession.builder 
      .master("local") 
      .appName("chispa") 
      .getOrCreate()
  )

@pytest.fixture(scope='session')
def spark_session():
    return spark_session_factory()