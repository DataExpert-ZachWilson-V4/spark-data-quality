def test_can_import_queries():
    from src.jobs.Update_Actors_CT import Actors_CT
    assert Actors_CT is not None
    from src.jobs.Update_User_Devices_Cumulated import User_Devices_Cumulated
    assert User_Devices_Cumulated is not None
