from src.jobs.job_1 import query_1
from src.jobs.job_2 import query_2


def test_can_import_queries():
    assert query_1 is not None
    assert query_2 is not None
