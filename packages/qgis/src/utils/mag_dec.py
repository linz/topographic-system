from datetime import datetime

from qgis.core import QgsExpression, QgsExpressionContext, QgsExpressionContextScope, QgsPoint

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

    scope = QgsExpressionContextScope()
    scope.setVariable("model_name", MODEL_NAME)
    scope.setVariable("model_path", MODEL_PATH)

    scope.setVariable("lat", lat)
    scope.setVariable("lon", lon)

    scope.setVariable("year", year)
    scope.setVariable("month", month)
    scope.setVariable("day", day)
    scope.setVariable("hour", HOUR)
    scope.setVariable("minute", MINUTE)
    scope.setVariable("second", SECOND)

    scope.setVariable("height", HEIGHT)

    context = QgsExpressionContext()
    context.appendScope(scope)

    expr = QgsExpression(
        "magnetic_declination(@model_name, make_datetime(@year, @month, @day, @hour, @minute, @second), @lat, @lon, @height, @model_path)"
    )
    if expr.hasParserError():
        raise RuntimeError(expr.parserErrorString())

    result = expr.evaluate(context)

    if expr.hasEvalError():
        raise RuntimeError(expr.evalErrorString())

    if not isinstance(result, float):
        raise TypeError("The calculated magnetic declination value is not a float number.")

    return result
