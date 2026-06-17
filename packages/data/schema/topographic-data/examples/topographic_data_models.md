# Topographic Data Models

Total models: 41

## Models

- [airport](#airport)
- [bridge_line](#bridge_line)
- [building](#building)
- [building_point](#building_point)
- [coastline](#coastline)
- [contour](#contour)
- [descriptive_text](#descriptive_text)
- [fence_line](#fence_line)
- [ferry_crossing](#ferry_crossing)
- [geographic_name](#geographic_name)
- [island](#island)
- [landcover](#landcover)
- [landcover_line](#landcover_line)
- [landcover_point](#landcover_point)
- [landuse](#landuse)
- [landuse_line](#landuse_line)
- [marine](#marine)
- [nz_topo50_map_sheet](#nz_topo50_map_sheet)
- [place_point](#place_point)
- [railway_line](#railway_line)
- [railway_station](#railway_station)
- [relief_line](#relief_line)
- [relief_point](#relief_point)
- [residential_area](#residential_area)
- [road_line](#road_line)
- [runway](#runway)
- [structure](#structure)
- [structure_line](#structure_line)
- [structure_point](#structure_point)
- [track_line](#track_line)
- [transport_point](#transport_point)
- [trig_point](#trig_point)
- [tunnel_line](#tunnel_line)
- [utility_line](#utility_line)
- [utility_point](#utility_point)
- [vegetation](#vegetation)
- [vegetation_line](#vegetation_line)
- [vegetation_point](#vegetation_point)
- [water](#water)
- [water_line](#water_line)
- [water_point](#water_point)

## airport

Represents the boundary of an airport facility.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## bridge_line

Represents bridges crossing roads, railways, walkways or water features.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| bridge_use | string | no | None |  | The use of the feature. |  |
| bridge_use2 | string | no | None |  | The use of the feature. |  |
| construction_type | string | no | None |  | The type of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## building

Represents the extent of a building. Captured from aerial imagery.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| building_use | string | no | None |  | The use of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## building_point

Represents the location of a building.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| building_use | string | no | None |  | The use of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| orientation | number | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## coastline

Represents the boundary between land and sea.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| coastline_type | string | no | None |  | The type of the feature. |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## contour

Represents elevation lines for terrain representation.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=32 |
| type | string | yes | required |  | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| definition | string | no | None |  |  |  |
| designation | string | no | None |  |  |  |
| formation | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=multilinestring |

## descriptive_text

Represents text annotations for labeling or descriptions.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| info_display | string | no | None |  |  |  |
| size | number | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## fence_line

Generated model for fence_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## ferry_crossing

Indicates ferry links to terminal locations.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## geographic_name

Represents named geographic locations and features.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| desc_code | string | no | None |  |  |  |
| size | number | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## island

Represents an island within a water body. Onshore and Offshore.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| group_name | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## landcover

Represents surface cover types such asice, moraine, sand. Vegetation layer managed forests etc.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| subtype | string | no | None |  | The type of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## landcover_line

Represents linear of land cover features such as dredge tailings.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## landcover_point

Represents land cover feature locations suck as rock outcrops.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| display | string | no | None |  |  |  |
| orientation | number | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## landuse

Represents areas designated for specific uses such as mines, racetracks.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| landuse_use | string | no | None |  | The use of the feature. |  |
| subtype | string | no | None |  | The subtype of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| substance_extracted | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## landuse_line

Represents linear of land use features suck as racetracks.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| landuse_use | string | no | None |  | The use of the feature. |  |
| subtype | string | no | None |  | The subtype of the feature. |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## marine

Represents features in the marine environment such as mangrives, reef, rocks and shoals.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| composition | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## nz_topo50_map_sheet

Represents the LINZ Topographic 1:50,000 map sheets.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | yes | required |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| sheet_code | string | yes | required | 21 |  |  |
| sheet_name | string | yes | required | 50 |  |  |
| x_origin | number | yes | required |  | The x-coordinate of the origin point. |  |
| y_origin | number | yes | required |  | The y-coordinate of the origin point. |  |
| example_point_id | string | yes | required |  | The identifier for an example point. |  |
| published_version | string | yes | required |  | The published version of the map sheet. |  |
| published_at | number | yes | required |  | The date when the map sheet was published. |  |
| updated_at | number | yes | required |  | The date when the map sheet was last updated. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## place_point

Represents a named place or locality.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| place_type | string | no | None |  | The type of the feature. |  |
| status | string | no | None |  |  |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| composition | string | no | None |  |  |  |
| description | string | no | None |  |  |  |
| orientation | number | no | None |  |  |  |
| substance_extracted | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## railway_line

Represents railway tracks. Dual tracks may be represented as single entity.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| railway_use | string | no | None |  | The use of the feature. |  |
| track_type | string | no | None |  | The type of the feature. |  |
| vehicle_type | string | no | None |  | The type of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| route | string | no | None |  |  |  |
| route2 | string | no | None |  |  |  |
| route3 | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## railway_station

Location of a railway station.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## relief_line

Represents terrain features such as cliffs.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| relief_use | string | no | None |  | The use of the feature. |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## relief_point

Represents terrain features such as peaks or saddles.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| display | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## residential_area

Represents areas primarily used for housing.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## road_line

Represents roads, including highways and streets.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| hierarchy | string | no | None |  |  |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| highway_number | string | no | None |  |  |  |
| lane_count | integer | no | None |  |  | precision=32 |
| surface | string | no | None |  |  |  |
| way_count | string | no | None |  |  |  |
| width_indicator | string | no | None |  |  |  |
| road_access | string | no | None |  |  |  |
| name_id | integer | no | None |  |  | precision=64 |
| rna_sufi | integer | no | None |  |  | precision=64 |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## runway

Represents airport runway areas.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| runway_use | string | no | None |  | The use of the feature. |  |
| status | string | no | None |  |  |  |
| surface | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## structure

Represents man-made structures other than buildings (e.g., reservoir, dry_dock, fish_farm, marine_farm, siphon, tank).

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| lid_type | string | no | None |  | The type of the feature. |  |
| subtype | string | no | None |  | The type of the feature. |  |
| species | string | no | None |  |  |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| stored_item | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## structure_line

Represents structural features such as  cableways, ladders, wharf, weirs etc.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| structure_use | string | no | None |  | The use of the feature. |  |
| species | string | no | None |  |  |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| material | string | no | None |  |  |  |
| material_conveyed | string | no | None |  |  |  |
| restrictions | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## structure_point

Represents small structures or structural features such as gates, masts tanks, windmills.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| structure_use | string | no | None |  | The use of the feature. |  |
| subtype | string | no | None |  | The subtype of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| location | string | no | None |  |  |  |
| height | number | no | None |  | The height of the feature in metres |  |
| orientation | number | no | None |  |  |  |
| material | string | no | None |  |  |  |
| restrictions | string | no | None |  |  |  |
| stored_item | string | no | None |  |  |  |
| wreck_of | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## track_line

Represents walking tracks, trails, or unsealed paths.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| track_use | string | no | None |  | The use of the feature. |  |
| track_type | string | no | None |  | The type of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## transport_point

Represents transport-related locations (e.g., bus stops, terminals).

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## trig_point

Represents a geodetic survey control point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| trig_type | string | no | None |  | The type of the feature. |  |
| name | string | no | None |  | The name of the feature. |  |
| code | string | no | None |  |  |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## tunnel_line

Represents tunnels for roads, railways, or utilities.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| tunnel_use | string | no | None |  | The use of the feature. |  |
| tunnel_use2 | string | no | None |  | The use of the feature. |  |
| subtype | string | no | None |  | The subtype of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## utility_line

Represents linear utility infrastructure such as  pipelines, power lines.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| utility_use | string | no | None |  | The use of the feature. |  |
| support_type | string | no | None |  | The type of the feature. |  |
| status | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| visibility | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## utility_point

Represents point utility infrastructure such as poles, towers.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| orientation | number | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## vegetation

Represents areas of vegetation landcover such as trees, scrub, vineyards.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| subtype | string | no | None |  | The type of the feature. |  |
| species | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## vegetation_line

Represents linear vegetation features such as shelter belts.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## vegetation_point

Represents individual trees or notable groups of trees.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | no | None |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |

## water

Represents water bodies such as  rivers, lakes.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| water_use | string | no | None |  | The use of the feature. |  |
| hierarchy | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| group_name | string | no | None |  |  |  |
| nzgb_feat_id | number | no | None |  |  |  |
| height | number | no | None |  | The height of the feature in metres |  |
| elevation | integer | no | None |  | The elevation above mean sea level. | precision=32 |
| perennial | string | no | None |  |  |  |
| temperature | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=polygon |

## water_line

Represents water features such as rivers, canals, drains.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| hierarchy | string | no | None |  |  |  |
| name | string | no | None |  | The name of the feature. |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=linestring |

## water_point

Represents water-related features such as springs.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | The unique identifier for the topographic feature. |  |
| t50_fid | integer | no | None |  | The unique identifier for the feature in the source database. | precision=64 |
| type | string | yes | required | 50 | The specific type of feature being represented (e.g., 'bridge_line', 'building'). |  |
| name | string | no | None |  | The name of the feature. |  |
| height | number | no | None |  | The height of the feature in metres |  |
| orientation | number | no | None |  |  |  |
| temperature_indicator | string | no | None |  |  |  |
| updated_at | number | yes | required |  | The date when the feature was last updated in the database. |  |
| created_at | number | yes | required |  | The date when the feature was created in the database. |  |
| geometry | object | yes | required |  | The geometry of the feature. | geometry_type=point |
