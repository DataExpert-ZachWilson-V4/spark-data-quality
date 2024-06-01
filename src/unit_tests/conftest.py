# Per https://docs.pytest.org/en/7.1.x/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files,
# conftest.py needs to be in either the same folder as the tests or an ancestor.
# It is not needed in the src/tests folder as the spark_session() fixture is not needed for the test in test_can_import.py 
import pytest
from pyspark.sql import SparkSession

def spark_session_factory(app_name: str) -> SparkSession:
  return (
      SparkSession.builder 
      .master("local") 
      .appName("chispa") 
      .getOrCreate()
  )

#Added an empty string argument for spark_session_factory as it is a function with a required parameter
@pytest.fixture(scope='session')
def spark_session():
    return spark_session_factory("")

