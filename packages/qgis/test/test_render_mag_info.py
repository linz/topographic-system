from datetime import datetime

from utils.render_mag_info import render_mag_info
from utils.types_mag_info import MagInfoRaw


def test_render_mag_info():
    # general
    date = datetime(year=2026, month=7, day=1)
    rate_years = 10

    input = MagInfoRaw(gm_degrees=23.4, gm_date=date, gm_rate_years=rate_years)
    output = render_mag_info(input)

    assert output["gm_degrees"] == "23½"
    assert output["gm_mils"] == "418"
    assert output["gm_year"] == str(date.year)
    assert output["gm_rate_years"] == str(rate_years)

    # preserve minus sign, remove ".0"
    input = MagInfoRaw(gm_degrees=-1.0, gm_date=date, gm_rate_years=rate_years)
    output = render_mag_info(input)
    assert output["gm_degrees"] == "-1"
    assert output["gm_mils"] == "-18"

    # round to -0.5, preserve sign, replace "0.5" with "½"
    input = MagInfoRaw(gm_degrees=-0.3, gm_date=date, gm_rate_years=rate_years)
    output = render_mag_info(input)
    assert output["gm_degrees"] == "-½"
    assert output["gm_mils"] == "-9"

    # remove ".0"
    input = MagInfoRaw(gm_degrees=0.0, gm_date=date, gm_rate_years=rate_years)
    output = render_mag_info(input)
    assert output["gm_degrees"] == "0"
    assert output["gm_mils"] == "0"

    # round to 0.5, replace "0.5" with "½"
    input = MagInfoRaw(gm_degrees=0.7, gm_date=date, gm_rate_years=rate_years)
    output = render_mag_info(input)
    assert output["gm_degrees"] == "½"
    assert output["gm_mils"] == "9"

    # remove ".0"
    input = MagInfoRaw(gm_degrees=1.0, gm_date=date, gm_rate_years=rate_years)
    output = render_mag_info(input)
    assert output["gm_degrees"] == "1"
    assert output["gm_mils"] == "18"