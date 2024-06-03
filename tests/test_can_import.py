import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

def test_can_import_queries():
    from jobs.job_1 import query_1
    assert query_1 is not None