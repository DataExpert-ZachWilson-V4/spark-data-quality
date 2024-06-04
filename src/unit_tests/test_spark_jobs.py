from chispa.dataframe_comparer import *

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")
HostActivity = namedtuple("HostActivity", "host_activity_dates")
Host = namedtuple("Host", "host host_activity_datelist date")

def test_job_1(spark_session):
    input_data = [
        Team(1, "GSW", "Warriors", "San Francisco", "Chase Center", 1900),
        Team(1, "GSW", "Bad Warriors", "San Francisco", "Chase Center", 1900)
    ]

    input_dataframe = spark=spark_session()
    spark.createDataFrame(input_data)
    actual_df = job_1(spark_session, input_dataframe)
    expected_output = [
        TeamVertex(
            identifier=1,
            type='team',
            properties={
                'abbreviation': 'GSW',
                'nickname': 'Warriors',
                'city': 'San Francisco',
                'arena': 'Chase Center',
                'year_founded': '1900'
            }
        )
    ]
    expected_df = spark_session.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)

def test_job_2(spark_session):
    input_data = [
        Host("www.zachwilson.tech", [HostActivity("2023-01-02","2023-01-01")], "2023-01-01"),
        Host("www.zachwilson.tech", [HostActivity("2023-01-03")], "2023-01-01"),
        Host("admin.zachwilson.tech", [HostActivity("2023-01-02","2023-01-01")], "2022-12-31")
    ]

    input_dataframe = spark=spark_session()
    spark.createDataFrame(input_data)
    actual_df = job_2(spark_session, input_dataframe)
    expected_output = [
        Host("www.zachwilson.tech", [HostActivity("2023-01-02","2023-01-01")], "2023-01-01"),
        Host("admin.zachwilson.tech", [HostActivity("2023-01-02","2023-01-01")], "2023-01-01")
    ]
    expected_df = spark_session.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)