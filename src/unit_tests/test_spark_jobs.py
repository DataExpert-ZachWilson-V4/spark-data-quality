from chispa.dataframe_comparer import *
from ..jobs.actors_history_scd_example import do_actor_scd_transformation
from ..jobs.dedupe_nba_game_details import do_device_transformation
from collections import namedtuple
ActorSeason = namedtuple("actor", "actor_id","film votes rating film_id", "quality_class", is_active, current_year)
ActorScd = namedtuple("actor", "quality_class", is_active, start_date, end_date, current_year)


def test_scd_generation(spark):
    source_data = [
        ActorSeason("Adam Rodriguez", "nm0735226", "Impostor", 22662, 6.2, "tt0160399", average, true, 2001),
        ActorSeason("Adrian Grenier", "nm0004978", "Harvard Man", 4225, 4.9, "tt0242508", average, true, 2001),
        ActorSeason("Alexander Skarsgard", "nm0002907", "Zoolander", 247922,6.6, "tt0196229", average, true, 2001),
        ActorSeason("Amy Locane", "nm0000504", "Bad Karma", 627,3.4, "tt0271984", bad, true, 2001)
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actor_scd_transformation(spark, source_df)
    expected_data = [
        ActorScd("Adam Garcia","average", true, 2001, 2001, 2021),
        ActorScd("Albert Brooks", 'good', true, 2001, 2001, 2021),
        ActorScd("Alexandra Holden", 'bad', true, 2001, 2001, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)


DeviceOutput = namedtuple(user_id, "browser_type", "dates_active", "date")
Device = namedtuple(user_id, "browser_type", "dates_active", "date")
	

def test_device_generation(spark):
    input_data = [
        Device(5890109, "curl", ["2023-01-09"], "2023-01-09"),
        Device(696863716, "Other", ["2023-01-09"], "2023-01-09")
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df2 = do_device_transformation(spark, input_dataframe)
    expected_output = [
        DeviceOutput(1867926153, "Googlebot", ["2023-01-09"], "2023-01-09")
    ]
    expected_df2 = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df2, expected_df2, ignore_nullable=True)
