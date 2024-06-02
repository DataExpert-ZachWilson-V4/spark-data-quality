

from datetime import datetime


def to_dt(time_str: str, is_date: bool=False) -> datetime:
    event_dtf = "%Y-%m-%d %H:%M:%S UTC"
    if is_date:
        event_dtf = "%Y-%m-%d"
    return datetime.strptime(time_str, event_dtf)