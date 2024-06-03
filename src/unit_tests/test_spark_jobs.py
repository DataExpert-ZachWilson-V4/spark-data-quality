from chispa.dataframe_comparer import *
from chispa.dataframe_comparer import assert_df_equality
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, LongType
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

PlayerGameDetails = namedtuple("PlayerGameDetails", "player_name team_id reb ast")
PlayerReboundsAssists = namedtuple("PlayerGameDetails", "player_name team_id number_of_games total_rebounds total_assists")
    
def test_job_1(spark_session):
    source_data = [
        PlayerGameDetails("Michael Jordan", '112233', 13, 4),
        PlayerGameDetails("Michael Jordan", '112233', 2, 1),
        PlayerGameDetails("Michael Jordan", '112233', 3, 2),
        PlayerGameDetails("Michael Jordan", '112233', 14, 3),
        PlayerGameDetails("Scottie Pippen", '223344', 2, 3),
        PlayerGameDetails("Scottie Pippen", '223344', 5, 1),
        PlayerGameDetails("Scottie Pippen", '223344', 5, 5),
        PlayerGameDetails("Scottie Pippen", '223344', 6, 4)
    ]
    source_df = spark_session.createDataFrame(source_data)

    actual_df = job_1(spark_session, source_df)
    expected_data = [
        PlayerReboundsAssists("Michael Jordan", '112233',4, 32, 10),
        PlayerReboundsAssists("Scottie Pippen", '223344',4, 18, 13)
    ]
    schema1 = StructType([
        StructField('player_name', StringType(), True),
        StructField('team_id', StringType(), True),
        StructField('number_of_games', LongType(), False),
        StructField('total_rebounds', LongType(), True),
        StructField('total_assists', LongType(), True)
    ])    
    expected_df = spark_session.createDataFrame(expected_data, schema=schema1)
    assert_df_equality(actual_df.sort("player_name", "team_id"), expected_df.sort("player_name", "team_id"))

InputCumulative = namedtuple("InputCumulative", "user_id browser_type dates_active date")
InputWebEvents = namedtuple("InputWebEvents", "user_id device_id event_time")
InputDevices = namedtuple ("InputDevices", "device_id browser_type")
OutputCumulative = namedtuple("OutputCumulative", "user_id browser_type dates_active date")

def test_job_2(spark_session):
    
    current_date = "2021-06-08"
    cumulated_table_name: str = "user_devices_cumulated"
    web_event_table_name: str = "web_events"
    device_table_name: str = "devices"
    
    cumulative_schema = StructType([
    StructField("user_id", LongType(), nullable=True),
    StructField("browser_type", StringType(), nullable=True),
    StructField("dates_active", ArrayType(StringType(), containsNull=True), nullable=True),
    StructField("date", StringType(), nullable=False)
    ])
    
    input_cumulative_data = [
    InputCumulative(1967566579, "Python-urllib",["2021-06-07"], "2021-06-07"),
    InputCumulative(1272828233, "Chrome", [None,"2021-06-06"], "2021-06-07"),
    InputCumulative(694175222, "AhrefsBot", ["2021-06-07"], "2021-06-07")
    ]
    cumulated_df = spark_session.createDataFrame(input_cumulative_data, cumulative_schema)
    
    web_event_schema = StructType([
        StructField("user_id", LongType(), nullable=True),
        StructField("device_id", LongType(), nullable=True),
        StructField("event_time", StringType(), nullable=True)
    ])
    input_web_event_data = [
    InputWebEvents(1967566579, -1138341683,"2021-06-08 23:57:37.316 UTC"),
    InputWebEvents(1272828233, -643696601,"2021-06-08 00:10:52.986 UTC"),
    InputWebEvents(694175222, 1847648591,"2021-06-08 00:15:29.251 UTC")
    ]
    web_event_df = spark_session.createDataFrame(input_web_event_data, web_event_schema)
    
    device_schema = StructType([
        StructField("device_id", LongType(), nullable=True),
        StructField("browser_type", StringType(), nullable=True)
    ])
        
    input_device_data = [
    InputDevices(-1138341683, "Python-urllib"),
    InputDevices(-643696601, "Chrome"),
    InputDevices(1847648591, "AhrefsBot")
    ]
    device_df = spark_session.createDataFrame(input_device_data, device_schema)

    actual_df = job_2(spark_session, cumulated_df, cumulated_table_name, web_event_df, web_event_table_name, device_df, device_table_name, current_date)
    actual_df = actual_df.orderBy("user_id", "browser_type") 
    
    expected_data = [
    OutputCumulative(1967566579, "Python-urllib",["2021-06-08", "2021-06-07"], "2021-06-08"),        
    OutputCumulative(1272828233, "Chrome", ["2021-06-08", None,"2021-06-06"], "2021-06-08"),
    OutputCumulative(694175222, "AhrefsBot", ["2021-06-08", "2021-06-07"], "2021-06-08")
    ]
    expected_df = spark_session.createDataFrame(expected_data, cumulative_schema)
    expected_df = expected_df.orderBy("user_id", "browser_type")
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)