from chispa.dataframe_comparer import *

from ..jobs.job_1 import job_1
from collections import namedtuple

TeamVertex = namedtuple("TeamVertex", "identifier type properties")
Team = namedtuple("Team", "team_id abbreviation nickname city arena yearfounded")

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
        