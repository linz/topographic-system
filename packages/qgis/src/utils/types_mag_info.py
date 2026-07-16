from datetime import datetime
from typing import TypedDict


class MagInfoRaw(TypedDict):
    gm_degrees: float
    gm_date: datetime
    gm_rate_years: float


class MagInfoRender(TypedDict):
    gm_degrees: str
    gm_mils: str
    gm_year: str
    gm_rate_years: str
