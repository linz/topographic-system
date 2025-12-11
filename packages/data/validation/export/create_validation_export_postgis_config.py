import json


def options_layer_generic():
    feature_not_on_layers = [
        {
            "table": "release62.railway_station",
            "intersection_table": "release62.railway_line",
            "layername": "stations-not-on-railway-line",
            "message": "Railway station point features must fall on a railway line",
        }
    ]
    feature_not_contains_layers = [
        {
            "table": "release62.airport",
            "intersection_table": "release62.runway",
            "layername": "runway-not-in-airport-polygons",
            "message": "Runway features must fall within airport polygon features",
        }
    ]
    line_not_on_feature_layers = [
        {
            "line_table": "release62.bridge_line",
            "intersection_table": "release62.road_line",
            "layername": "bridges-not-on-road-line",
            "message": "Bridge line features must fall on a road line",
            "where": "bridge_use = 'vehicle'",
        },
        {
            "line_table": "release62.bridge_line",
            "intersection_table": "release62.railway_line",
            "layername": "bridges-not-on-railway-line",
            "message": "Bridge line features must fall on a railway line",
            "where": "bridge_use = 'train'",
        },
        {
            "line_table": "release62.tunnel_line",
            "intersection_table": "release62.railway_line",
            "layername": "tunnels-not-on-railway-line",
            "message": "Train tunnel line features must fall on a railway line",
            "where": "tunnel_use = 'train'",
        },
    ]
    line_not_touches_feature_layers = [
        {
            "line_table": "release62.ferry_crossing",
            "intersection_table": "release62.structure_line",
            "layername": "ferry-not-touches-structure-line",
            "message": "Ferry line features must not touch structure line features",
        }
    ]
    feature_in_layers = [
        {
            "table": "release62.building_point",
            "intersection_table": "release62.building",
            "layername": "building-points-in-building-polygons",
            "message": "Building point features must not fall within building polygon features",
        },
        {
            "table": "release62.building_point",
            "intersection_table": "release62.residential_area",
            "layername": "building-points-in-residential",
            "message": "Building point features must not fall within residential area polygon features",
        },
        {
            "table": "release62.vegetation_line",
            "intersection_table": "release62.road_line",
            "layername": "shelterbelt-crossing-roads",
            "message": "Shelterbelt line features must not cross road line features",
            "where": "feature_type = 'shelter_belt'",
        },
    ]
    self_intersection_layers = [
        {
            "table": "release62.building",
            "layername": "building-validation",
            "message": "Building features must not self-intersect",
        },
        {
            "table": "release62.vegetation",
            "layername": "vegetation-validation",
            "message": "Vegetation features must not self-intersect",
        },
        {
            "table": "release62.residential_area",
            "layername": "residential_area-validation",
            "message": "Residential area features must not self-intersect",
        },
        {
            "table": "release62.landcover",
            "layername": "landcover-validation",
            "message": "Landcover features must not self-intersect",
        },
        {
            "table": "release62.island",
            "layername": "island-validation",
            "message": "Island features must not self-intersect",
        },
        {
            "table": "release62.structure",
            "layername": "structure-validation",
            "message": "Structure features must not self-intersect",
        },
        {
            "table": "release62.water",
            "layername": "water-validation",
            "message": "Water features must not self-intersect",
        },
    ]
    null_columns = [
        {
            "table": "release62.descriptive_text",
            "column": "info_display",
            "message": "Descriptive text features must have an info_display attribute",
        },
        {
            "table": "release62.descriptive_text",
            "column": "size",
            "message": "Descriptive text features must have a size attribute",
        },
        {
            "table": "release62.geographic_name",
            "column": "name",
            "message": "Geographic name features must have a name attribute",
        },
        {
            "table": "release62.geographic_name",
            "column": "desc_code",
            "message": "Geographic name features must have a desc_code attribute",
        },
        {
            "table": "release62.geographic_name",
            "column": "size",
            "message": "Geographic name features must have a size attribute",
        },
        {
            "table": "release62.building_point",
            "column": "orientation",
            "message": "Building point features must have an orientation attribute",
        },
        {
            "table": "release62.trig_point",
            "column": "code",
            "message": "Trig point features must have a code attribute",
        },
        {
            "table": "release62.road_line",
            "column": "lane_count",
            "where": "status NOT IN ('proposed', 'non-under construction')",
            "message": "Road line features must have a lane_count attribute where status is not proposed or non-under construction",
        },
        {
            "table": "release62.road_line",
            "column": "name",
            "where": "surface is not null",
            "message": "Road line features with a surface attribute must have a name attribute where surface is not null",
        },
        {
            "table": "release62.railway_line",
            "column": "vehicle_type",
            "message": "Railway line features must have a vehicle_type attribute",
        },
        {
            "table": "release62.track_line",
            "column": "track_use",
            "message": "Track line features must have a track_use attribute",
        },
        {
            "table": "release62.tunnel_line",
            "column": "tunnel_use",
            "message": "Tunnel line features must have a tunnel_use attribute",
        },
        {
            "table": "release62.runway",
            "column": "runway_use",
            "message": "Runway features must have a runway_use attribute",
        },
        {
            "table": "release62.runway",
            "column": "surface",
            "message": "Runway features must have a surface attribute",
        },
        {
            "table": "release62.physical_infrastructure_point",
            "column": "orientation",
            "where": "feature_type = 'pylon'",
            "message": "Physical infrastructure point features must have an orientation attribute for type pylon",
        },
        {
            "table": "release62.physical_infrastructure_line",
            "column": "support_type",
            "where": "feature_type = 'powerline'",
            "message": "Physical infrastructure line features must have a support_type attribute for type powerline",
        },
        {
            "table": "release62.landcover",
            "column": "visibility",
            "where": "feature_type = 'mine'",
            "message": "Landcover features must have a visibility attribute for type mine",
        },
        {
            "table": "release62.water_point",
            "column": "orientation",
            "where": "(feature_type = 'waterfall' OR feature_type = 'soakhole')",
            "message": "Water point features must have an orientation attribute for types waterfall and soakhole",
        },
        {
            "table": "release62.water_point",
            "column": "height",
            "where": "feature_type = 'waterfall'",
            "message": "Water point features must have a height attribute for type waterfall",
        },
        {
            "table": "release62.water_point",
            "column": "temperature_indicator",
            "where": "feature_type = 'spring'",
            "message": "Water point features must have a temperature_indicator attribute for type spring",
        },
        {
            "table": "release62.water_line",
            "column": "height",
            "where": "(feature_type = 'waterfall' OR feature_type = 'waterfall_edge')",
            "message": "Water line features must have a height attribute for types waterfall and waterfall_edge",
        },
        {
            "table": "release62.water",
            "column": "elevation",
            "where": "(feature_type = 'lagoon' OR feature_type = 'lake')",
            "message": "Water features must have an elevation attribute for types lagoon and lake",
        },
        {
            "table": "release62.water",
            "column": "elevation",
            "where": "feature_type = 'waterfall'",
            "message": "Waterfall features must have a elevation attribute for type waterfall",
        },
    ]

    query_rules = [
        {
            "table": "release62.vegetation",
            "column": "species",
            "where": "feature_type = 'exotic'",
            "rule": "species IN ('coniferous', 'non-coniferous')",
            "message": "Exotic vegetation must have species as coniferous or non-coniferous",
        }
    ]

    return (
        feature_not_on_layers,
        feature_in_layers,
        line_not_on_feature_layers,
        line_not_touches_feature_layers,
        feature_not_contains_layers,
        self_intersection_layers,
        null_columns,
        query_rules,
    )


(
    feature_not_on_layers,
    feature_in_layers,
    line_not_on_feature_layers,
    line_not_touches_feature_layers,
    feature_not_contains_layers,
    self_intersect_layers,
    null_columns,
    query_rules,
) = options_layer_generic()
data = {
    "feature_not_on_layers": feature_not_on_layers,
    "feature_in_layers": feature_in_layers,
    "line_not_on_feature_layers": line_not_on_feature_layers,
    "line_not_touches_feature_layers": line_not_touches_feature_layers,
    "feature_not_contains_layers": feature_not_contains_layers,
    "self_intersect_layers": self_intersect_layers,
    "null_columns": null_columns,
    "query_rules": query_rules,
}
with open("./validation/system/validation_postgis_config.json", "w") as f:
    json.dump(data, f, indent=4)

with open("./validation/system/validation_postgis_config.json", "r") as f:
    loaded_data = json.load(f)
    feature_not_on_layers = loaded_data["feature_not_on_layers"]
    feature_in_layers = loaded_data["feature_in_layers"]
    line_not_on_feature_layers = loaded_data["line_not_on_feature_layers"]
    line_not_touches_feature_layers = loaded_data["line_not_touches_feature_layers"]
    feature_not_contains_layers = loaded_data["feature_not_contains_layers"]
    self_intersect_layers = loaded_data["self_intersect_layers"]
    null_columns = loaded_data["null_columns"]
    query_rules = loaded_data["query_rules"]

print(query_rules)
# loaded_data is a dictionary containing all the config sections
