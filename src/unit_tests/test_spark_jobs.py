from chispa.dataframe_comparer import *
from ..jobs.job_1 import job_1
from ..jobs.job_2 import job_2
from collections import namedtuple

job1_input = namedtuple("job1_input", "actor	actor_id	films	quality_class	is_active	current_year")
job1_output = namedtuple("job1_output", "actor_id	actor	quality_class	is_active	start_date	end_date	current_year")
job2_input = namedtuple("job2_input", "identifier type properties")
job2_output = namedtuple("job2_output", "team_id abbreviation nickname city arena yearfounded")

def test_job1(spark):
    input_data = [
        job1_input(
          actor="Famous Actor", 
          actor_id="nm9999999", 
          films=[["Phobias","tt8129682",289,3.7]],
          quality_class="bad",
          is_active=True, 
          current_year=2018),
        job1_input(
          actor="Famous Actor", 
          actor_id="nm9999999", 
          films=[["Phobias","tt8129682",289,3.7]],
          quality_class="bad",
          is_active=True, 
          current_year=2018),
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = do_team_vertex_transformation(spark, input_dataframe)
    expected_output = [
        job1_output(
            actor_id="nm9999999",
            actor="Famous Actor",
            quality_class="bad",
            is_active=True,
            start_date=2018,
            end_date=2021,
            current_year=2021
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
