import pytest
from pyspark.sql import SparkSession
from src.jobs.job_1 import job_1, query_1

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

if __name__ == "__main__":
    pytest.main(["-s", __file__])
