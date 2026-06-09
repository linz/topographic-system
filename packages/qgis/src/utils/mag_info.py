from typing import TypedDict

from qgis.core import (
    QgsCoordinateReferenceSystem,
    QgsCoordinateTransform,
    QgsFeature,
    QgsPoint,
    QgsPointXY,
    QgsProject,
)

from utils.mag_dec import calculate_magnetic_declination

TARGET_EPSG_CODE = 4326  # WGS 84 (World Geodetic System 1984)


class MagInfoFloat(TypedDict):
    gm_degrees: float
    gm_mils: float
    gm_year: int
    gm_rate_years: int


class MagInfoStr(TypedDict):
    gm_degrees: str
    gm_mils: str
    gm_year: str
    gm_rate_years: str


def _to_point(pt_xy: QgsPointXY):
    return QgsPoint(pt_xy.x(), pt_xy.y(), 0)


def _degrees_to_mils(degrees: float) -> float:
    return degrees * 6400 / 360


def calculate_mag_info(project: QgsProject, topo_map_sheet: str, sheet_code: str) -> MagInfoFloat:
    nz_topo50_map_sheet = project.mapLayersByName(topo_map_sheet)[0]

    features = list(nz_topo50_map_sheet.getFeatures())

    for feature in features:
        if not isinstance(feature, QgsFeature):
            raise TypeError("feature is not a QgsFeature")

        if feature.attribute("sheet_code") != sheet_code:
            continue

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
        decl = calculate_magnetic_declination(center_transformed_point)

        # gm angle
        gm_degrees = decl - conv
        gm_mils = _degrees_to_mils(gm_degrees)

        return MagInfoFloat(gm_degrees=gm_degrees, gm_mils=gm_mils, gm_year=2026, gm_rate_years=7)

    raise RuntimeError(f"failed to find a feature for sheet_code: {sheet_code}")


def render_mag_info(mag_info: MagInfoFloat) -> MagInfoStr:
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
    gm_year_str = f"{mag_info['gm_year']}"

    # gm_rate_years
    gm_rate_years_str = f"{mag_info['gm_rate_years']}"

    return MagInfoStr(
        gm_degrees=gm_degrees_str, gm_mils=gm_mils_str, gm_year=gm_year_str, gm_rate_years=gm_rate_years_str
    )
