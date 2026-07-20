from datetime import datetime

from PyQt6.QtCore import QDate, QDateTime
from qgis.core import QgsCoordinateReferenceSystem, QgsCoordinateTransform, QgsFeature, QgsPoint, QgsPointXY, QgsProject

from utils.mag_dec import calculate_magnetic_declination, calculate_rate_of_change
from utils.types_mag_info import MagInfoRaw

TARGET_EPSG_CODE = 4326  # WGS 84 (World Geodetic System 1984)


def calculate_mag_info(
    project: QgsProject, feature: QgsFeature, source_crs: QgsCoordinateReferenceSystem
) -> MagInfoRaw:
    # date (rounded to july 1st)
    published_at = feature.attribute("published_at")

    if isinstance(published_at, QDate):
        date = published_at.toPyDate()
    elif isinstance(published_at, QDateTime):
        date = published_at.toPyDateTime()
    else:
        raise TypeError(f"published_at is not a QDate or QDateTime. Actual type: {type(published_at)}")

    date_rounded = datetime(date.year, month=7, day=1)

    # geometry (source crs)
    geometry = feature.geometry()
    bbox = geometry.boundingBox()

    # center (source crs)
    center = bbox.center()

    # prepare transform
    target_crs = QgsCoordinateReferenceSystem.fromEpsgId(TARGET_EPSG_CODE)
    transform = QgsCoordinateTransform(source_crs, target_crs, project)

    # center (target crs)
    center_transformed_point_xy = transform.transform(center)
    center_transformed_point = _to_point(center_transformed_point_xy)

    # grid convergence
    conv = source_crs.factors(center_transformed_point).meridianConvergence()

    # magnetic declination
    decl = calculate_magnetic_declination(center_transformed_point, date_rounded)

    # rate of change
    rate_years = calculate_rate_of_change(center_transformed_point, date_rounded)

    # gm angle
    gm_degrees = decl - conv

    return MagInfoRaw(gm_degrees=gm_degrees, gm_date=date_rounded, gm_rate_years=rate_years)


def _to_point(pt_xy: QgsPointXY):
    return QgsPoint(pt_xy.x(), pt_xy.y(), 0)
