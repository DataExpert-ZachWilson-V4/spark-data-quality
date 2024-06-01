
#Updated path to query_1 as there is no src/job_1 folder
def test_can_import_queries():
    from src.jobs.job_1 import query_1
    assert query_1 is not None