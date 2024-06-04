

def test_can_import_queries():
    from src.job_1 import query_1 # type: ignore
    assert query_1 is not None