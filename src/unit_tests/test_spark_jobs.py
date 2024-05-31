from chispa.dataframe_comparer import *
from jobs.job_1 import incremental_update_host_cumulated
from jobs.job_2 import deduplicate_game_details
import datetime
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType 


#Namedtuples to represent the tables/dataframes
HostsCumulated = namedtuple("HostsCumulated", ["host", "host_activity_datelist", "date"])
WebEvents = namedtuple("WebEvents", ["host", "event_time"])

Gamedetails = namedtuple("HostsCumulated", ["game_id", "team_id", "player_id", 'points'])
Gamedetails_Deduped = namedtuple("HostsCumulated", ["game_id", "team_id", "player_id", 'points', 'row_number'])



def test_deduplicate_game_details(spark_session):
    test_deduped_game_details_data = [
    Gamedetails('game1', 'team1', 'player1', 10),
    Gamedetails('game1', 'team1', 'player1', 10),
    Gamedetails('game2', 'team2', 'player2', 10),
    Gamedetails('game1', 'team1', 'player2', 10)
        ]
    
    results_deduped_game_details_data = [
    Gamedetails_Deduped('game1', 'team1', 'player1', 10, 1),
    Gamedetails_Deduped('game1', 'team1', 'player2', 10,1),
    Gamedetails_Deduped('game2', 'team2', 'player2', 10,1)
        ]
    
    expected_df_schema  = StructType([  #Needed because actual data type for row_number is Integer while the test data row_nubmer is being inferred as a long type
        StructField("game_id", StringType(), True),
        StructField("team_id", StringType(), True),
        StructField("player_id", StringType(), True),
        StructField("points", LongType(), True),
        StructField("row_number", IntegerType(), False)
    ])
    

    df_game_details = spark_session.createDataFrame(test_deduped_game_details_data)
    df_game_details.createOrReplaceTempView("df_game_details")
    actual_df = deduplicate_game_details(spark_session,df_game_details)
    expected_df = spark_session.createDataFrame(results_deduped_game_details_data, schema = expected_df_schema)

    assert_df_equality(actual_df,expected_df)



def test_incremental_update_host_cumulated(spark_session):
    """Host data should be combined from webevents and we should see a union of hosts between both tables """
    test_data_hosts_cumulated = [
    HostsCumulated('host1', [datetime.date(2023,1,1)], datetime.date(2023,1,1,)),
    HostsCumulated('host2', [datetime.date(2023,1,1)], datetime.date(2023,1,1))
]
    results_data_hosts_cumulated = [
    HostsCumulated('host1', [datetime.date(2023, 1, 2), datetime.date(2023, 1, 1)], datetime.date(2023, 1, 2)),
    HostsCumulated('host2', [None, datetime.date(2023, 1, 1)], datetime.date(2023, 1, 2)),
    HostsCumulated('host3', [datetime.date(2023, 1, 2)], datetime.date(2023, 1, 2))
]

    data_web_events = [
        WebEvents('host1', '2023-01-02 10:00:00'),
        WebEvents('host3', '2023-01-02 12:00:00')
    ]

    
    df_hosts_cumulated = spark_session.createDataFrame(test_data_hosts_cumulated)
    df_web_events = spark_session.createDataFrame(data_web_events)
    df_hosts_cumulated.createOrReplaceTempView("hosts_cumulated")
    df_web_events.createOrReplaceTempView("web_events")


    actual_df = incremental_update_host_cumulated(spark_session, df_hosts_cumulated,df_web_events)
    expected_df = spark_session.createDataFrame(results_data_hosts_cumulated)
    assert_df_equality(actual_df, expected_df)

