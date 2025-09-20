import pandas as pd
from datetime import datetime
import textwrap


def format_datetime(dt: int) -> str:
    """Format a datetime object to the iCalendar UTC format."""
    utc_dt = datetime.utcfromtimestamp(dt)
    return utc_dt.strftime("%Y%m%dT%H%M%SZ")


def create_ics_file(data_row: pd.Series) -> str:
    import uuid

    content = textwrap.dedent(
        f"""
        BEGIN:VCALENDAR
        VERSION:2.0
        PRODID:-//Your Organization//Your Product//EN
        BEGIN:VEVENT
        UID:{uuid.uuid4()}
        DTSTAMP:{format_datetime(int(datetime.utcnow().timestamp()))}
        DTSTART:{format_datetime(data_row.Date_de_debut)}
        DTEND:{format_datetime(data_row.Date_de_fin)}
        SUMMARY:{data_row.Intitule}
        LOCATION:{data_row.Lieu}
        DESCRIPTION:{data_row.Description}
        END:VEVENT
        END:VCALENDAR
    """
    )

    filepath = f"/tmp/ics_{data_row.Index}.ics"
    with open(filepath, "w") as f:
        f.write(content)

    return filepath
