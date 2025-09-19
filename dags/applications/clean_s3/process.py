import re
from datetime import datetime
from typing import Optional


def is_file_to_delete(s3_object_date, date_to_compare) -> bool:
    if s3_object_date < date_to_compare:
        return True
    return False


def check_date_format(s3_date: str) -> bool:
    if re.fullmatch(r"\d{8}", s3_date):
        try:
            datetime.strptime(s3_date, "%Y%m%d")
            return True
        except ValueError:
            print(f"{s3_date} is 8 digits but it doesn't respect AAAAMMDD format")
            return False
    return False


def check_hour_format(s3_hour: str) -> bool:
    if re.fullmatch(r"\d{2}h\d{2}", s3_hour):
        return True
    print(f"{s3_hour} doesn't respect HHhMM format (Example: 13h26)")
    return False


def safe_parse_date(date_str: str, is_date_format: bool) -> Optional[datetime]:
    if is_date_format:
        return datetime.strptime(date_str, "%Y%m%d")
    return None
