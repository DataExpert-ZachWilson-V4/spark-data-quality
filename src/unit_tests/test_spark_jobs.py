from datetime import datetime
import pytest
from pyspark.sql import DataFrame

from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2

# util for date creation
def to_dt(time_str: str, is_date: bool=False) -> datetime:
    event_dtf = "%Y-%m-%d %H:%M:%S UTC"
    if is_date:
        event_dtf = "%Y-%m-%d"
    return datetime.strptime(time_str, event_dtf)

def test_job_one(spark_session) -> None:

    # set up values mock data for input tables
    # events table - testing with one user 2116161230
    events = [
      {"user_id": 2116161230, "device_id": 1324700293, "host": "a", "event_time": to_dt("2023-08-14 12:54:05 UTC")},
      {"user_id": 2116161230, "device_id": 1324700293, "host": "b", "event_time": to_dt("2023-08-14 02:24:21 UTC")},
      {"user_id": 2116161230, "device_id": 1602222, "host": "b", "event_time": to_dt("2023-08-14 02:11:00 UTC")},
      {"user_id": 2116161230, "device_id": 1602222, "host": "a", "event_time": to_dt("2023-08-15 04:10:05 UTC")},
    ]
    events_df: DataFrame = spark_session.createDataFrame(events)
    events_df.createOrReplaceTempView("mock_events")

    # devices table - testing with one user 2116161230
    devices = [
      {"device_id": 1324700293,	"browser_type": "Chrome", "os_type": "Mac OS X", "device_type": "Other"},
      {"device_id": 1602222, "browser_type": "Samsung Internet", "os_type": "Android", "device_type": "Samsung SM-A715F"},
    ]
    devices_df: DataFrame = spark_session.createDataFrame(devices)
    devices_df.createOrReplaceTempView("mock_devices")

    # mock output
    event_dt = to_dt('2023-08-14', is_date=True)
    user_devices = [
        {"user_id": 2116161230, "browser_type": "Chrome", "dates_active": [event_dt], "date": event_dt},
        {"user_id": 2116161230, "browser_type": "Samsung Internet", "dates_active": [event_dt], "date": event_dt},
    ]
    schema = "user_id: bigint, browser_type: string, dates_active: array<timestamp>, date: timestamp"
    expected_user_devices_df: DataFrame = spark_session.createDataFrame(user_devices, schema)
    expected_user_devices_df.createOrReplaceTempView("mock_user_devices")

   # test the job_1 function with the inputs given
    actual_user_devices_df = job_1(
      spark_session=spark_session,
      events_input_table_name="mock_events",
      devices_input_table_name="mock_devices",
      output_table_name="mock_user_devices",
      current_date='2023-08-14'
    )

    compare_df = expected_user_devices_df.exceptAll(actual_user_devices_df)
    assert compare_df.count() == 0, \
        f"User devices don't match. \nExpected: {expected_user_devices_df.show()}" + \
            f"\nActual: {actual_user_devices_df.show()}"


def test_job_two(spark_session) -> None:

    # set up values mock data for input tables
    # actor_films table - testing with two actors in 1914 Lillian Gish, Harold Lloyd
    actor_films = [
      {
        "actor": "Lillian Gish",
        "actor_id": "nm0001273",
        "film": "Romola",
        "year": 1924,
        "votes": 165,
        "rating": 6.2,
        "film_id": "tt0015289"
      },
      {
        "actor": "Harold Lloyd",
        "actor_id": "nm0516001",
        "film": "Girl Shy",
        "year": 1924,
        "votes": 3138,
        "rating": 7.7,
        "film_id": "tt0014945"
      },
      {
        "actor": "Harold Lloyd",
        "actor_id": "nm0516001",
        "film": "Hot Water",
        "year": 1924,
        "votes": 1258,
        "rating": 7.1,
        "film_id": "tt0015002"
      },
    ]
    actor_films_df = spark_session.createDataFrame(actor_films)
    actor_films_df.createOrReplaceTempView("mock_actor_films")

    # mock output
    actors_history = [
       {
          "actor_id": "nm0001273",
          "actor": "Lillian Gish",
          "films": [["tt0015289","Romola",1924,165,6.2]],
          "quality_class": "average",
          "is_active": True,
          "current_year": 1924
       },
       {
          "actor_id": "nm0516001",
          "actor": "Harold Lloyd",
          "films": [["tt0014945","Girl Shy",1924,3138,7.7],["tt0015002","Hot Water",1924,1258,7.1]],
          "quality_class": "good",
          "is_active": True,
          "current_year": 1924
       },
    ]
    schema = "actor_id: string, actor: string, films: array<array<string>>, quality_class: string, is_active: boolean, current_year: bigint"
    expected_actors_history_df: DataFrame = spark_session.createDataFrame(actors_history, schema)
    expected_actors_history_df.createOrReplaceTempView("mock_actors_history")

    print("we made it past making the mock input table")

    # test the job_2 function with the inputs given
    actual_actors_history_df = job_2(
      spark_session=spark_session,
      input_table_name="mock_actor_films",
      output_table_name="mock_actors_history",
      load_year=1924
    )

    compare_df = expected_actors_history_df.exceptAll(actual_actors_history_df)
    assert compare_df.count() == 0, \
        f"Actor history records don't match. \nExpected: {expected_actors_history_df.show()}" + \
        f"\nActual: {actual_actors_history_df.show()}"

if __name__ == "__main__":
    pytest.main([__file__])