from datetime import datetime
from typing import TypedDict

from PyQt6.QtCore import QDate
from qgis.core import QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsFeature, QgsPoint, QgsPointXY, QgsProject

from utils.mag_dec import calculate_magnetic_declination, calculate_rate_of_change

TARGET_EPSG_CODE = 4326  # WGS 84 (World Geodetic System 1984)


class MagInfoRaw(TypedDict):
    gm_degrees: float
    gm_mils: float
    gm_date: datetime
    gm_rate_years: float


class MagInfoRender(TypedDict):
    gm_degrees: str
    gm_mils: str
    gm_year: str
    gm_rate_years: str


def _to_point(pt_xy: QgsPointXY):
    return QgsPoint(pt_xy.x(), pt_xy.y(), 0)


def _degrees_to_mils(degrees: float) -> float:
    return degrees * 6400 / 360


def calculate_mag_info(project: QgsProject, topo_map_sheet: str, sheet_code: str) -> MagInfoRaw:
    nz_topo50_map_sheet = project.mapLayersByName(topo_map_sheet)[0]

    features = list(nz_topo50_map_sheet.getFeatures())

    for feature in features:
        if not isinstance(feature, QgsFeature):
            raise TypeError("feature is not a QgsFeature")

        if feature.attribute("sheet_code") != sheet_code:
            continue

        # date (rounded to july 1st)
        published_at = feature.attribute("published_at")
        if not isinstance(published_at, QDate):
            raise TypeError("published_at is not a QDate")

        date = datetime(published_at.toPyDate().year, month=7, day=1)

        # geometry (source crs)
        geometry = feature.geometry()
        bbox = geometry.boundingBox()

        # center (source crs)
        center = bbox.center()

        # prepare transform
        source_crs = nz_topo50_map_sheet.crs()
        target_crs = QgsCoordinateReferenceSystem.fromEpsgId(TARGET_EPSG_CODE)

        transform = QgsCoordinateTransform(source_crs, target_crs, project)

        # center (target crs)
        center_transformed_point_xy = transform.transform(center)
        center_transformed_point = _to_point(center_transformed_point_xy)

        # grid convergence
        conv = source_crs.factors(center_transformed_point).meridianConvergence()

        # magnetic declination
        decl = calculate_magnetic_declination(center_transformed_point, date)

        # rate of change
        rate_years = calculate_rate_of_change(center_transformed_point, date)

        # gm angle
        gm_degrees = decl - conv
        gm_mils = _degrees_to_mils(gm_degrees)

        return MagInfoRaw(gm_degrees=gm_degrees, gm_mils=gm_mils, gm_date=date, gm_rate_years=rate_years)

    raise RuntimeError(f"failed to find a feature for sheet_code: {sheet_code}")


def render_mag_info(mag_info: MagInfoRaw) -> MagInfoRender:
    # gm_degrees
    gm_degrees_rounded = round(mag_info["gm_degrees"] * 2) / 2  # nearest 0.5

    if gm_degrees_rounded.is_integer():
        gm_degrees_str = f"{int(gm_degrees_rounded)}"
    else:
        gm_degrees_str = f"{int(gm_degrees_rounded)}½"  # format 0.5 as half symbol

    # gm_mils
    gm_mils_rounded = _degrees_to_mils(gm_degrees_rounded)
    gm_mils_int = int(gm_mils_rounded)
    gm_mils_str = f"{gm_mils_int}"

    # gm_year
    gm_year_str = f"{mag_info['gm_date'].year}"

    # gm_rate_years
    gm_rate_years_rounded = round(mag_info["gm_rate_years"])
    gm_rate_years_str = f"{gm_rate_years_rounded}"

    return MagInfoRender(
        gm_degrees=gm_degrees_str, gm_mils=gm_mils_str, gm_year=gm_year_str, gm_rate_years=gm_rate_years_str
    )
