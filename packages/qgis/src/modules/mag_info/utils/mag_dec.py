from datetime import datetime

from qgis.core import QgsExpression, QgsPoint

MODEL_NAME = "igrf13"
MODEL_PATH = "/usr/qgis/assets/models"

NOW = datetime.now()
YEAR = NOW.year
MONTH = NOW.month
DAY = NOW.day
HOUR = 0
MINUTE = 0
SECOND = 0

HEIGHT = 0


# expects a lat-lon pair in WGS84 (EPSG:4326)
def calculate_magnetic_declination(
    point: QgsPoint,
    year=YEAR,
    month=MONTH,
    day=DAY,
) -> float:
    lat = point.y()  # y axis, vertical axis, north/south, latitude
    lon = point.x()  # x axis, horizontal axis, east/west, longitude

    expr = QgsExpression(
        f"magnetic_declination({MODEL_NAME}, make_datetime({year}, {month}, {day}, {HOUR}, {MINUTE}, {SECOND}), {lat}, {lon}, {HEIGHT}, {MODEL_PATH})"
    )
    if expr.hasParserError():
        raise RuntimeError(expr.parserErrorString())

    result = expr.evaluate()

    if expr.hasEvalError():
        raise RuntimeError(expr.evalErrorString())

    if not isinstance(result, float):
        raise TypeError("The calculated magnetic declination value is not a float number.")

    return result
