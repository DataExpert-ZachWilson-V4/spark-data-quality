def test_can_import_queries():
    try:
        from src.jobs.job_1 import job_1
        from src.jobs.job_2 import job_2
    except ImportError as e:
        assert False, f"ImportError: {str(e)}"

