from datetime import datetime
from typing import NotRequired, TypedDict

from qgis.core import QgsExpression, QgsExpressionContext, QgsExpressionContextScope, QgsPoint

DEFAULTS = {"MODEL_NAME": "igrf14", "MODEL_PATH": "/usr/qgis/models", "HEIGHT": 0}


class MagDecOptions(TypedDict):
    date: datetime
    model_name: NotRequired[str]
    model_path: NotRequired[str]
    height: NotRequired[float]


# expects a lat-lon pair in WGS84 (EPSG:4326)
def calculate_magnetic_declination(
    point: QgsPoint,
    options: MagDecOptions,
) -> float:
    lat = point.y()  # y axis, vertical axis, north/south, latitude
    lon = point.x()  # x axis, horizontal axis, east/west, longitude

    date = options["date"]
    model_name = options.get("model_name", DEFAULTS["MODEL_NAME"])
    model_path = options.get("model_path", DEFAULTS["MODEL_PATH"])
    height = options.get("height", DEFAULTS["HEIGHT"])

    scope = QgsExpressionContextScope()
    scope.setVariable("model_name", model_name)
    scope.setVariable("model_path", model_path)

    scope.setVariable("lat", lat)
    scope.setVariable("lon", lon)

    scope.setVariable("year", date.year)
    scope.setVariable("month", date.month)
    scope.setVariable("day", date.day)
    scope.setVariable("hour", 0)
    scope.setVariable("minute", 0)
    scope.setVariable("second", 0)

    scope.setVariable("height", height)

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


def calculate_rate_of_change(point: QgsPoint, options: MagDecOptions) -> float:
    date = options["date"]

    num_degrees = 0.5
    num_years = 20

    date_before = date.replace(year=date.year - int(num_years / 2))
    date_after = date.replace(year=date.year + int(num_years / 2))

    decl_before = calculate_magnetic_declination(point, {**options, "date": date_before})
    decl_after = calculate_magnetic_declination(point, {**options, "date": date_after})

    decl_delta = decl_after - decl_before

    rate_of_change = (num_degrees * num_years) / decl_delta

    return rate_of_change
