from chispa.dataframe_comparer import assert_df_equality
from ..jobs.job_2 import job_2
from collections import namedtuple
from datetime import date

UserDevicesCumulated = namedtuple(
    "UserDevicesCumulated",
    "user_id browser_type dates_active date"
)

UserBinaryActivity = namedtuple(
    "UserBinaryActivity",
    "user_id browser_type history_int history_in_binary",
)

input_data = [
    UserDevicesCumulated(
        user_id=1,
        browser_type="Chrome",
        dates_active=[date(2023,1,2)],
        date= date(2023,1,4)
    ),
]


def test_job_2(spark_session):
    fake_input_data = spark_session.createDataFrame(input_data)
    actual_output_data = job_2(spark_session, fake_input_data)
    expected_output_data = [
        UserBinaryActivity(
            user_id=1,
            browser_type="Chrome",
            history_int=536870912,
            history_in_binary="100000000000000000000000000000"
        ),
    ]
    expected_output_data_df = spark_session.createDataFrame(expected_output_data)
    assert_df_equality(
        actual_output_data, expected_output_data_df, ignore_nullable=True
    )