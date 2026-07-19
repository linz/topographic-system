from utils.types_mag_info import MagInfoRaw, MagInfoRender


def render_mag_info(mag_info: MagInfoRaw) -> MagInfoRender:
    # gm_degrees
    gm_degrees_rounded = round(mag_info["gm_degrees"] * 2) / 2  # nearest 0.5
    gm_degrees_str = str(gm_degrees_rounded).replace(".0", "").replace(".5", "½")

    if gm_degrees_str == "0½":
        gm_degrees_str = "½"
    elif gm_degrees_str == "-0½":
        gm_degrees_str = "-½"

    # gm_mils
    gm_mils_raw = _degrees_to_mils(gm_degrees_rounded)
    gm_mils_rounded = round(gm_mils_raw)
    gm_mils_str = str(gm_mils_rounded)

    # gm_year
    gm_year_str = str(mag_info["gm_date"].year)

    # gm_rate_years
    gm_rate_years_rounded = round(mag_info["gm_rate_years"])
    gm_rate_years_str = str(gm_rate_years_rounded)

    return MagInfoRender(
        gm_degrees=gm_degrees_str, gm_mils=gm_mils_str, gm_year=gm_year_str, gm_rate_years=gm_rate_years_str
    )


def _degrees_to_mils(degrees: float) -> float:
    return degrees * 6400 / 360
