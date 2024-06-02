import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2
from pyspark.sql.functions import *

@pytest.fixture(scope="session")
def spark():
    spark = SparkSession.builder.master("local[1]").appName("yavuz_unit_test").getOrCreate()
    yield spark
    spark.stop()
@pytest.fixture(scope="function")
def actors_table(spark):
    data = [
        ("Gloria Swanson", "nm0841797", [[1920,"Why Change Your Wife?",1342,6.7,"tt0011865"],[1920,"Something to Think About",579,5.2,"tt0011716"],[1919,"Male and Female",1740,7,"tt0010418"],[1919,"Don't Change Your Husband",1131,6.6,"tt0010071"],[1919,"For Better, for Worse",697,5.8,"tt0010137"],[1918,"Shifting Sands",702,5.1,"tt0009608"]], "bad", True, 1920),
        ("Noah Beery Jr.", "nm0000890", [[1920,"The Mark of Zorro",2247,7.1,"tt0011439"]], "good", True, 1920),
        ("Gloria Swanson", "nm0841797", [[1923,"Zaza",799,6.2,"tt0014636"],[1922,"Beyond the Rocks",2226,6.7,"tt0012938"],[1921,"The Affairs of Anatol",1215,6.6,"tt0011909"],[1920,"Why Change Your Wife?",1342,6.7,"tt0011865"],[1920,"Something to Think About",579,5.2,"tt0011716"],[1919,"Male and Female",1740,7,"tt0010418"],[1919,"Don't Change Your Husband",1131,6.6,"tt0010071"],[1919,"For Better, for Worse",697,5.8,"tt0010137"],[1918,"Shifting Sands",702,5.1,"tt0009608"]], "average", True, 1923),
        ("Noah Beery Jr.", "nm0000890", [[1920,"The Mark of Zorro",2247,7.1,"tt0011439"]], None, False, 1923),
        ("Gloria Swanson", "nm0841797", [[1922,"Beyond the Rocks",2226,6.7,"tt0012938"],[1921,"The Affairs of Anatol",1215,6.6,"tt0011909"],[1920,"Why Change Your Wife?",1342,6.7,"tt0011865"],[1920,"Something to Think About",579,5.2,"tt0011716"],[1919,"Male and Female",1740,7,"tt0010418"],[1919,"Don't Change Your Husband",1131,6.6,"tt0010071"],[1919,"For Better, for Worse",697,5.8,"tt0010137"],[1918,"Shifting Sands",702,5.1,"tt0009608"]], "average", True, 1922),
        ("Noah Beery Jr.", "nm0000890", [[1920,"The Mark of Zorro",2247,7.1,"tt0011439"]], None, False, 1922),
        ("Gloria Swanson", "nm0841797", [[1918,"Shifting Sands",702,5.1,"tt0009608"]], "bad", True, 1918),
        ("Gloria Swanson", "nm0841797", [[1919,"Male and Female",1740,7,"tt0010418"],[1919,"Don't Change Your Husband",1131,6.6,"tt0010071"],[1919,"For Better, for Worse",697,5.8,"tt0010137"],[1918,"Shifting Sands",702,5.1,"tt0009608"]], "average", True, 1919),
        ("Gloria Swanson", "nm0841797", [[1921,"The Affairs of Anatol",1215,6.6,"tt0011909"],[1920,"Why Change Your Wife?",1342,6.7,"tt0011865"],[1920,"Something to Think About",579,5.2,"tt0011716"],[1919,"Male and Female",1740,7,"tt0010418"],[1919,"Don't Change Your Husband",1131,6.6,"tt0010071"],[1919,"For Better, for Worse",697,5.8,"tt0010137"],[1918,"Shifting Sands",702,5.1,"tt0009608"]], "average", True, 1921),
        ("Noah Beery Jr.", "nm0000890", [[1920,"The Mark of Zorro",2247,7.1,"tt0011439"]], None, False, 1921),
    ]
    schema = ["actor", "actor_id", "films", "quality_class", "is_active", "current_year"]
    actors_df = spark.createDataFrame(data, schema)
    actors_df.createOrReplaceTempView("actors")
    yield actors_df
    spark.catalog.dropTempView("actors")
@pytest.fixture(scope="function")
def web_events_table(spark):
    data = [
        ('user001','device01','2023-01-01 00:46:23.897 UTC'),
        ('user001','device01','2023-01-01 00:46:23.897 UTC'),
        ('user001','device01','2023-01-02 00:46:23.897 UTC'),
        ('user001','device01','2023-01-03 00:46:23.897 UTC'),
        ('user001','device02','2023-01-03 00:46:23.897 UTC'),
        ('user001','device02','2023-01-03 00:46:23.897 UTC'),
        ('user002','device02','2023-01-01 00:46:23.897 UTC'),
        ('user002','device02','2023-01-02 00:46:23.897 UTC'),
        ('user002','device02','2023-01-02 00:46:23.897 UTC'),
        ('user002','device02','2023-01-02 00:46:23.897 UTC'),
        ('user002','device03','2023-01-02 00:46:23.897 UTC'),
    ]
    schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('device_id', StringType(), True),
        StructField('event_time', StringType(), True)
        ])
    web_events_df = spark.createDataFrame(data, schema).withColumn("event_time", col("event_time").cast("timestamp"))
    web_events_df.createOrReplaceTempView("web_events")
    yield web_events_df
    spark.catalog.dropTempView("web_events")
@pytest.fixture(scope="function")
def user_devices_cumulated_table(spark):
    schema = schema = StructType([
        StructField('user_id', StringType(), True),
        StructField('browser_type', StringType(), True),
        StructField('dates_active', ArrayType(DateType(), True), True),
        StructField('date', DateType(), True)
        ])
    user_devices_cumulated_df = spark.createDataFrame([], schema)
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")
    yield user_devices_cumulated_df
    spark.catalog.dropTempView("user_devices_cumulated")
@pytest.fixture(scope="function")
def devices_table(spark):
    data = [
        ('device01', 'chrome'),
        ('device02', 'ie'),
        ('device03', 'chrome'),
    ]
    schema = StructType([
        StructField('device_id', StringType(), True),
        StructField('browser_type', StringType(), True)
        ])
    devices_df = spark.createDataFrame(data, schema)
    devices_df.createOrReplaceTempView("devices")
    yield devices_df
    spark.catalog.dropTempView("devices")
def test_job_1(spark, actors_table):
    actual = job_1(spark, "actors")
    expected_data = [
        ("Gloria Swanson", "nm0841797", "bad", True, 1918, 1918, 1921),
        ("Gloria Swanson", "nm0841797", "average", True, 1919, 1919, 1921),
        ("Gloria Swanson", "nm0841797", "bad", True, 1920, 1920, 1921),
        ("Gloria Swanson", "nm0841797", "average", True, 1921, 1921, 1921),
        ("Noah Beery Jr.", "nm0000890", "good", True, 1920, 1920, 1921),
        ("Noah Beery Jr.", "nm0000890", None, False, 1921, 1921, 1921)
    ]
    expected_schema = ["actor", "actor_id", "quality_class", "is_active", "start_date", "end_date", "current_year"]
    expected_df = spark.createDataFrame(data=expected_data, schema=expected_schema)
    assert actual.collect() == expected_df.collect()
def test_job_2(spark, web_events_table, devices_table, user_devices_cumulated_table):
    actual = job_2(spark, "user_devices_cumulated", "2023-01-01")
    expected_data = [
        ('user002', 'chrome', ['2023-01-01'], '2023-01-01'),
        ('user001', 'chrome', ['2023-01-01'], '2023-01-01'),
        ('user002', 'ie', ['2023-01-01'], '2023-01-01')
    ]
    expected_schema = ["user_id", "browser_type", "dates_active", "date"]
    expected_df_not_transformed = spark.createDataFrame(expected_data, expected_schema)
    expected_df = expected_df_not_transformed\
        .withColumn("dates_active", array(to_date(col("dates_active")[0], 'yyyy-MM-dd')))\
        .withColumn("date", to_date(col("date"), 'yyyy-MM-dd'))    
    assert actual.collect() == expected_df.collect()
if __name__ == "__main__":
    pytest.main(["-s", __file__])
