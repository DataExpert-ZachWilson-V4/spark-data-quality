from chispa.dataframe_comparer import *
from collections import namedtuple
from datetime import date, datetime


from src.jobs.job_1 import job_1
from src.jobs.job_2 import job_2

#bootcamp table -actor_films
actor_films = namedtuple("actor_films", "actor actor_id film year votes rating film_id")
#user table actors
actors = namedtuple("actors", "actor actor_id films quality_class is_active current_year")

# we only have one record for 1916 as current year so using the same
def test_job_one(spark):
    input_actor_data = [
        actor_films("Lillian Gish","nm0001273","Intolerance: Love's Struggle Throughout the Ages",1916,14378,7.7,"tt0006864")
    ]

    last_year_data = [

        actors("Lillian Gish", "nm0001273",[["The Birth of a Nation",22989,6.3,"tt0004972",1915]],"average",true,1916) # type: ignore
    ]

    expected_data = [

        actors("Lillian Gish","nm0001273",[["Intolerance: Love's Struggle Throughout the Ages", 14378,7.7,"tt0053137",1916]],"good",true,1916) # type: ignore
    ]

    input_dataframe = spark.createDataFrame(input_actor_data)
    input_dataframe.createOrReplaceTempView("actor_films")
    last_season_dataframe = spark.createDataFrame(last_year_data)
    last_season_dataframe.createOrReplaceTempView("actors")
    expected_output_dataframe = spark.createDataFrame(expected_data)
    actual_df = job_1(spark,"actors")
    assert_df_equality(actual_df,expected_output_dataframe,ignore_nullable=True)

    
    #test job 2
devices = namedtuple("devices", "device_id browser_type os_type device_type")
web_events = namedtuple("web_events", "user_id device_id referrer host url event_time")
user_devices_cumulated = namedtuple("user_devices_cumulated", "user_id browser_type dates_active date")

def test_job_2(spark_two):

    input_data_events = [web_events(-111622330,-1016146441,"","www.eczachly.com","/admin/kindeditor/asp/upload_json.asp?dir=file","2021-01-18 21:11:54.201 UTC")]
    input_data_devices = [devices(-1016146441, "Chrome","Other","Spider" )]
    expected_data = [user_devices_cumulated(user_id = -111622330,browser_type = 'Chrome',dates_active = [datetime.strptime("2021-01-18", "%Y-%m-%d").date()],date = datetime.strptime("2021-01-18", "%Y-%m-%d").date())]
         
    input_events_df = spark_two.createDataFrame(input_data_events)
    input_events_df.createOrReplaceTempView("web_events")
    input_devices_df = spark_two.createDataFrame(input_data_devices)
    input_devices_df.createOrReplaceTempView("devices")
    actual_df = job_2(spark_two, "user_devices_cumulated")
    expected_df = spark_two.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
         
