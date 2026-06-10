from datetime import datetime

from qgis.core import QgsExpression, QgsExpressionContext, QgsExpressionContextScope, QgsPoint

MODEL_NAME = "igrf13"
MODEL_PATH = "/usr/qgis/models"


# expects a lat-lon pair in WGS84 (EPSG:4326)
def calculate_magnetic_declination(
    point: QgsPoint,
    date: datetime,
) -> float:
    lat = point.y()  # y axis, vertical axis, north/south, latitude
    lon = point.x()  # x axis, horizontal axis, east/west, longitude

    scope = QgsExpressionContextScope()
    scope.setVariable("model_name", MODEL_NAME)
    scope.setVariable("model_path", MODEL_PATH)

    scope.setVariable("lat", lat)
    scope.setVariable("lon", lon)

    scope.setVariable("year", date.year)
    scope.setVariable("month", date.month)
    scope.setVariable("day", date.day)
    scope.setVariable("hour", 0)
    scope.setVariable("minute", 0)
    scope.setVariable("second", 0)

    scope.setVariable("height", 0)

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


def calculate_rate_of_change(point: QgsPoint, date: datetime, num_degrees=0.5) -> float:
    # period: 10 years before, 10 years after

    date_before = date.replace(year=date.year - 10)
    date_after = date.replace(year=date.year + 10)

    decl_before = calculate_magnetic_declination(point, date_before)
    decl_after = calculate_magnetic_declination(point, date_after)
    decl_delta = decl_after - decl_before

    rate_of_change = (num_degrees * 20) / decl_delta

    return rate_of_change
