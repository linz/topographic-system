# Topographic Data Models

Total models: 50

## Models

- [AuditFields](#auditfields)
- [ProductIdentity](#productidentity)
- [TopoIdentity](#topoidentity)
- [airport](#airport)
- [bridge_line](#bridge_line)
- [building](#building)
- [building_point](#building_point)
- [coastline](#coastline)
- [contour](#contour)
- [descriptive_text](#descriptive_text)
- [fence_line](#fence_line)
- [ferry_crossing](#ferry_crossing)
- [ferry_line](#ferry_line)
- [geographic_name](#geographic_name)
- [island](#island)
- [landcover](#landcover)
- [landcover_line](#landcover_line)
- [landcover_point](#landcover_point)
- [landuse](#landuse)
- [landuse_line](#landuse_line)
- [marine](#marine)
- [nztopo50_carto_text](#nztopo50_carto_text)
- [nztopo50_dms_grid](#nztopo50_dms_grid)
- [nztopo50_grid](#nztopo50_grid)
- [nztopo50_map_sheet](#nztopo50_map_sheet)
- [place_point](#place_point)
- [railway_line](#railway_line)
- [railway_point](#railway_point)
- [railway_station](#railway_station)
- [relief](#relief)
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

## AuditFields

Capture / change-tracking columns shared by most features.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |

## ProductIdentity

Generated model for ProductIdentity.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |

## TopoIdentity

Identity columns shared by every file in the dataset.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |

## airport

Generated model for airport.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | yes | required |  |  |  |

## bridge_line

Generated model for bridge_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  |  |
| use1 | unknown | yes | required |  |  |  |
| use2 | unknown | yes | required |  |  |  |
| construction_type | unknown | yes | required |  |  |  |
| status | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## building

Generated model for building.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| building_use | string | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## building_point

Generated model for building_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| building_use | string | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| orientation | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## coastline

Generated model for coastline.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| elevation | integer | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## contour

Generated model for contour.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | integer | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| elevation | integer | no | None |  |  |  |
| definition | string | no | None |  |  |  |
| designation | unknown | no | None |  |  |  |
| formation | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## descriptive_text

Generated model for descriptive_text.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| nzgb_id | unknown | no | None |  |  |  |
| info_display | string | no | None |  |  |  |
| size | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## fence_line

Generated model for fence_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## ferry_crossing

Generated model for ferry_crossing.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## ferry_line

Generated model for ferry_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## geographic_name

Generated model for geographic_name.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| desc_code | string | no | None |  |  |  |
| size | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## island

Generated model for island.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| group_name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## landcover

Generated model for landcover.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## landcover_line

Generated model for landcover_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## landcover_point

Generated model for landcover_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| elevation | integer | no | None |  |  |  |
| display | unknown | no | None |  |  |  |
| orientation | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## landuse

Generated model for landuse.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| landuse_use | unknown | no | None |  |  |  |
| subtype | unknown | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| substance_extracted | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## landuse_line

Generated model for landuse_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| landuse_use | unknown | no | None |  |  |  |
| subtype | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## marine

Generated model for marine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| composition | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## nztopo50_carto_text

Generated model for nztopo50_carto_text.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| example_point_id | string | no | None |  | id (UUID) of the example feature. |  |
| full_text | string | no | None |  |  |  |
| text_bend | integer | no | None |  |  |  |
| text_char_spacing_distance | integer | no | None |  |  |  |
| text_colour | integer | no | None |  |  |  |
| text_font | unknown | no | None |  |  |  |
| text_height | number | no | None |  |  |  |
| text_orientation | number | no | None |  |  |  |
| text_placement | integer | no | None |  |  |  |
| text_size_type | integer | no | None |  |  |  |
| text_stretch_length | integer | no | None |  |  |  |
| text_string | string | no | None |  |  |  |
| text_word_spacing_distance | integer | no | None |  |  |  |
| font | unknown | no | None |  |  |  |
| style | unknown | no | None |  |  |  |
| colour | unknown | no | None |  |  |  |
| size | number | no | None |  |  |  |
| placement | unknown | no | None |  |  |  |
| offset | number | no | None |  |  |  |
| textanchor | unknown | no | None |  |  |  |
| labelanchor | number | no | None |  |  |  |
| charplace | unknown | no | None |  |  |  |
| chardistance | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## nztopo50_dms_grid

Generated model for nztopo50_dms_grid.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| direction | unknown | no | None |  |  |  |
| value | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## nztopo50_grid

Generated model for nztopo50_grid.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| direction | unknown | no | None |  |  |  |
| value | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## nztopo50_map_sheet

Generated model for nztopo50_map_sheet.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| sheet_code | string | yes | required |  |  |  |
| sheet_name | string | yes | required |  |  |  |
| x_origin | number | yes | required |  |  |  |
| y_origin | number | yes | required |  |  |  |
| example_point_id | string | yes | required |  | topo_id (UUID) of the example feature. |  |
| published_version | string | yes | required |  |  |  |
| published_at | object | yes | required |  |  |  |
| updated_at | object | yes | required |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## place_point

Generated model for place_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| place_type | unknown | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| elevation | integer | no | None |  |  |  |
| composition | unknown | no | None |  |  |  |
| description | string | no | None |  |  |  |
| orientation | number | no | None |  |  |  |
| substance_extracted | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## railway_line

All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps. 
Where a railway line is located close to a road, the line held in the data and shown on the printed map 
may be offset from the road sufficient that the two symbols are recognisable at 1:50,000.

Multiple sidings may be held in the data and shown on the printed maps as a single feature

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  |  |
| railway_use | unknown | yes | required |  |  |  |
| track_type | unknown | yes | required |  |  |  |
| vehicle_type | unknown | yes | required |  |  |  |
| status | unknown | yes | required |  |  |  |
| name | string | yes | required |  | The name of the railway line if known |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## railway_point

Generated model for railway_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## railway_station

Generated model for railway_station.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## relief

Generated model for relief.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| height | number | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## relief_line

Generated model for relief_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| relief_use | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| height | number | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## relief_point

Generated model for relief_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| display | unknown | yes | required |  |  |  |
| elevation | integer | yes | required |  |  |  |
| height | number | yes | required |  |  |  |
| orientation | number | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## residential_area

Generated model for residential_area.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## road_line

Generated model for road_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| hierarchy | string | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| highway_number | string | no | None |  |  |  |
| lane_count | integer | no | None |  |  |  |
| surface | unknown | no | None |  |  |  |
| way_count | unknown | no | None |  |  |  |
| width_indicator | string | no | None |  |  |  |
| road_access | unknown | no | None |  |  |  |
| name_id | unknown | no | None |  |  |  |
| rna_sufi | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## runway

Generated model for runway.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| runway_use | unknown | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| surface | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## structure

Generated model for structure.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| lid_type | string | yes | required |  |  |  |
| subtype | unknown | yes | required |  |  |  |
| species | unknown | yes | required |  |  |  |
| status | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| stored_item | unknown | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## structure_line

Generated model for structure_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| structure_use | unknown | yes | required |  |  |  |
| species | unknown | yes | required |  |  |  |
| status | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| material | unknown | yes | required |  |  |  |
| material_conveyed | unknown | yes | required |  |  |  |
| restrictions | unknown | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## structure_point

Generated model for structure_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| structure_use | unknown | yes | required |  |  |  |
| tank_type | unknown | yes | required |  |  |  |
| subtype | unknown | yes | required |  |  |  |
| status | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| location | unknown | yes | required |  |  |  |
| height | number | yes | required |  |  |  |
| orientation | number | yes | required |  |  |  |
| material | unknown | yes | required |  |  |  |
| restrictions | unknown | yes | required |  |  |  |
| stored_item | unknown | yes | required |  |  |  |
| wreck_of | unknown | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## track_line

Generated model for track_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| track_use | unknown | no | None |  |  |  |
| track_type | unknown | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## transport_point

Generated model for transport_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## trig_point

Generated model for trig_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| trig_type | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| code | string | no | None |  |  |  |
| elevation | integer | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## tunnel_line

Generated model for tunnel_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| tunnel_use | unknown | no | None |  |  |  |
| tunnel_use2 | unknown | no | None |  |  |  |
| subtype | unknown | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## utility_line

Generated model for utility_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| utility_use | unknown | no | None |  |  |  |
| support_type | unknown | no | None |  |  |  |
| status | unknown | no | None |  |  |  |
| name | string | no | None |  |  |  |
| visibility | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## utility_point

Generated model for utility_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| orientation | number | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## vegetation

Generated model for vegetation.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | unknown | yes | required |  |  |  |
| subtype | unknown | no | None |  |  |  |
| species | unknown | no | None |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## vegetation_line

Generated model for vegetation_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUID for the feature. |  |
| updated_at | object | yes | required |  |  |  |
| created_at | object | yes | required |  | Default at write time: today. |  |
| t50_fid | unknown | no | None |  |  |  |
| type | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | WKB geometry. Validation only checks presence + non-null; semantic checks live elsewhere. |  |
| bbox | unknown | no | None |  |  |  |

## vegetation_point

Generated model for vegetation_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## water

Generated model for water.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| water_use | unknown | yes | required |  |  |  |
| hierarchy | string | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| group_name | string | yes | required |  |  |  |
| nzgb_feat_id | unknown | yes | required |  |  |  |
| height | number | yes | required |  |  |  |
| elevation | integer | yes | required |  |  |  |
| perennial | unknown | yes | required |  |  |  |
| temperature_indicator | unknown | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## water_line

Generated model for water_line.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| hierarchy | string | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## water_point

Generated model for water_point.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | string | yes | required |  | ISO Datetime of when the feature was created |  |
| updated_at | string | yes | required |  | ISO Datetime of when the feature was last updated |  |
| t50_fid | integer | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | unknown | yes | required |  |  |  |
| name | string | yes | required |  |  |  |
| orientation | number | yes | required |  |  |  |
| temperature_indicator | unknown | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | unknown | no | None |  | GeoParquet 1.1 covering bbox struct. |  |
