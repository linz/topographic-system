# Topographic Data Models

Total models: 53

## Models

- [Airport](#airport)
- [BridgeLine](#bridgeline)
- [Building](#building)
- [BuildingPoint](#buildingpoint)
- [Coastline](#coastline)
- [Contour](#contour)
- [DescriptiveText](#descriptivetext)
- [FenceLine](#fenceline)
- [FerryLine](#ferryline)
- [GeographicName](#geographicname)
- [Island](#island)
- [Landcover](#landcover)
- [LandcoverLine](#landcoverline)
- [LandcoverPointCoreTypes](#landcoverpointcoretypes)
- [Landuse](#landuse)
- [LanduseLine](#landuseline)
- [LandusePoint](#landusepoint)
- [Marine](#marine)
- [MarinePoint](#marinepoint)
- [Nztopo50CartoText](#nztopo50cartotext)
- [Nztopo50CoastlineIsland](#nztopo50coastlineisland)
- [Nztopo50DmsGrid](#nztopo50dmsgrid)
- [Nztopo50Grid](#nztopo50grid)
- [Nztopo50IceContour](#nztopo50icecontour)
- [Nztopo50MapSheet](#nztopo50mapsheet)
- [Nztopo50RockLine](#nztopo50rockline)
- [Nztopo50SeaPolygon](#nztopo50seapolygon)
- [PlacePoint](#placepoint)
- [RailwayLine](#railwayline)
- [RailwayPoint](#railwaypoint)
- [Relief](#relief)
- [ReliefLine](#reliefline)
- [ReliefPoint](#reliefpoint)
- [ResidentialArea](#residentialarea)
- [RoadLine](#roadline)
- [RockOutcrop](#rockoutcrop)
- [Runway](#runway)
- [Structure](#structure)
- [StructureLine](#structureline)
- [StructurePoint](#structurepoint)
- [TrackLine](#trackline)
- [TransportPoint](#transportpoint)
- [TrigPoint](#trigpoint)
- [TunnelLine](#tunnelline)
- [UtilityLine](#utilityline)
- [UtilityPoint](#utilitypoint)
- [Vegetation](#vegetation)
- [VegetationLine](#vegetationline)
- [VegetationPoint](#vegetationpoint)
- [Water](#water)
- [WaterLine](#waterline)
- [WaterPoint](#waterpoint)
- [BBox](#bbox)

## Airport

Generated model for Airport.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'airport' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## BridgeLine

Generated model for BridgeLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | Optional[string] | yes | required |  |  | enum: 'cablecar', 'farm', 'foot_traffic', 'train', 'vehicle' |
| subtype | Optional[string] | yes | required |  |  | enum: 'foot_traffic', 'train' |
| construction_type | Optional[string] | yes | required |  |  | enum: 'suspension', 'swing', 'trestle' |
| status | Optional[string] | yes | required |  |  | enum: 'closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Building

Generated model for Building.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'building' |
| subtype | Optional[string] | yes | required |  |  | enum: 'abattoir', 'camp', 'cement_works', 'church', 'energy_facility', 'factory', 'fertiliser_works', 'fire_lookout', 'forest_headquarters', 'gas_compound', 'greenhouse', 'gun_club', 'gun_emplacement', 'hall', 'homestead', 'hospital', 'hut', 'lodge', 'marae', 'mill', 'museum', 'observatory', 'polytechnic', 'power_generation', 'prison', 'private_hut', 'salt_works', 'school', 'shingle_works', 'shelter', 'silo', 'stamping_battery', 'substation', 'surf_club', 'university', 'visitor_centre', 'water_treatment_plant' |
| status | Optional[string] | yes | required |  |  | enum: 'derelict', 'historic', 'private' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'buildings' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## BuildingPoint

Generated model for BuildingPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'building' |
| subtype | Optional[string] | yes | required |  |  | enum: 'abattoir', 'camp', 'cement_works', 'church', 'energy_facility', 'factory', 'fertiliser_works', 'fire_lookout', 'forest_headquarters', 'gas_compound', 'greenhouse', 'gun_club', 'gun_emplacement', 'hall', 'homestead', 'hospital', 'hut', 'lodge', 'marae', 'mill', 'museum', 'observatory', 'polytechnic', 'power_generation', 'prison', 'private_hut', 'salt_works', 'school', 'shingle_works', 'shelter', 'silo', 'stamping_battery', 'substation', 'surf_club', 'university', 'visitor_centre', 'water_treatment_plant' |
| status | Optional[string] | yes | required |  |  | enum: 'derelict', 'historic', 'private' |
| name | Optional[string] | yes | required |  |  |  |
| orientation | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'buildings' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Coastline

Generated model for Coastline.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'coastline' |
| elevation | Optional[integer] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Contour

Generated model for Contour.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'contour' |
| elevation | Optional[integer] | yes | required |  |  |  |
| definition | Optional[string] | yes | required |  |  |  |
| designation | Optional[string] | yes | required |  |  | enum: 'supplementary' |
| formation | Optional[string] | yes | required |  |  | enum: 'depression' |
| theme | string | yes | required |  |  | enum: 'relief' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## DescriptiveText

Generated model for DescriptiveText.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'descriptive_text' |
| info_display | Optional[string] | yes | required |  |  |  |
| size | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'annotation' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## FenceLine

Generated model for FenceLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'fence' |
| theme | string | yes | required |  |  | enum: 'structures' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## FerryLine

Generated model for FerryLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'ferry_crossing' |
| subtype | Optional[string] | yes | required |  |  | enum: 'vehicle' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## GeographicName

Generated model for GeographicName.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'geographic_name' |
| name | Optional[string] | yes | required |  |  |  |
| desc_code | Optional[string] | yes | required |  |  |  |
| size | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'annotation' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Island

Generated model for Island.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'island' |
| name | Optional[string] | yes | required |  |  |  |
| group_name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Landcover

Generated model for Landcover.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'ice', 'moraine', 'moraine_wall', 'mud', 'sand', 'scree', 'shingle', 'swamp' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## LandcoverLine

Generated model for LandcoverLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'dredge_tailing' |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## LandcoverPointCoreTypes

Generated model for LandcoverPointCoreTypes.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cemetery', 'fumarole', 'swamp' |
| orientation | unknown | yes | required |  |  |  |
| elevation | unknown | yes | required |  |  |  |
| subtype | unknown | yes | required |  |  |  |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Landuse

Generated model for Landuse.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cemetery', 'cycle_track', 'dog_track', 'golf_course', 'gravel_pit', 'horse_track', 'landfill', 'mine', 'orchard', 'pumice_pit', 'quarry', 'racetrack', 'rifle_range', 'showground', 'sportsfield', 'vineyard', 'vehicle_track' |
| subtype | Optional[string] | yes | required |  |  | enum: 'training', 'opencast', 'underground' |
| status | Optional[string] | yes | required |  |  | enum: 'dangerous', 'disused', 'old' |
| name | Optional[string] | yes | required |  |  |  |
| substance_extracted | Optional[string] | yes | required |  |  | enum: 'bentonite', 'clay', 'coal', 'gold', 'gravel', 'ironsand', 'lime', 'limestone', 'metal', 'quartz', 'scheelite', 'shingle', 'silica_sand', 'silver', 'stone', 'zeolite' |
| theme | string | yes | required |  |  | enum: 'landuse' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## LanduseLine

Generated model for LanduseLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cemetery', 'cycle_track', 'dog_track', 'golf_course', 'gravel_pit', 'horse_track', 'landfill', 'mine', 'orchard', 'pumice_pit', 'quarry', 'racetrack', 'rifle_range', 'showground', 'sportsfield', 'vineyard', 'vehicle_track' |
| subtype | Optional[string] | yes | required |  |  | enum: 'training', 'opencast', 'underground' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landuse' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## LandusePoint

Generated model for LandusePoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cemetery', 'cycle_track', 'dog_track', 'golf_course', 'gravel_pit', 'horse_track', 'landfill', 'mine', 'orchard', 'pumice_pit', 'quarry', 'racetrack', 'rifle_range', 'showground', 'sportsfield', 'vineyard', 'vehicle_track' |
| subtype | Optional[string] | yes | required |  |  | enum: 'training', 'opencast', 'underground' |
| status | Optional[string] | yes | required |  |  | enum: 'dangerous', 'disused', 'old' |
| name | Optional[string] | yes | required |  |  |  |
| substance_extracted | Optional[string] | yes | required |  |  | enum: 'bentonite', 'clay', 'coal', 'gold', 'gravel', 'ironsand', 'lime', 'limestone', 'metal', 'quartz', 'scheelite', 'shingle', 'silica_sand', 'silver', 'stone', 'zeolite' |
| theme | string | yes | required |  |  | enum: 'landuse' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Marine

Generated model for Marine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'mangrove' |
| subtype | Optional[string] | yes | required |  |  | enum: 'coral', 'limestone', 'pumice', 'rock' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## MarinePoint

Generated model for MarinePoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'rock' |
| subtype | Optional[string] | yes | required |  |  | enum: 'coral', 'limestone', 'pumice', 'rock' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50CartoText

Generated model for Nztopo50CartoText.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| example_point_id | Optional[string] | yes | required |  | id (UUID) of the example feature. |  |
| full_text | Optional[string] | yes | required |  |  |  |
| text_bend | Optional[integer] | yes | required |  |  |  |
| text_char_spacing_distance | Optional[integer] | yes | required |  |  |  |
| text_colour | Optional[integer] | yes | required |  |  |  |
| text_font | Optional[string] | yes | required |  |  | enum: 'ATTrium-Italic', 'ATTriumMou-Cond', 'ATTriumMou-CondBold', 'ATTriumMou-CondItalic', 'ATTriumMou-Italic', 'ATTriumMou-Regular', 'Courier Bold Oblique' |
| text_height | Optional[number] | yes | required |  |  |  |
| text_orientation | Optional[number] | yes | required |  |  |  |
| text_placement | Optional[integer] | yes | required |  |  |  |
| text_size_type | Optional[integer] | yes | required |  |  |  |
| text_stretch_length | Optional[integer] | yes | required |  |  |  |
| text_string | Optional[string] | yes | required |  |  |  |
| text_word_spacing_distance | Optional[integer] | yes | required |  |  |  |
| font | Optional[string] | yes | required |  |  | enum: '', 'Nimbus Sans LINZ' |
| style | Optional[string] | yes | required |  |  | enum: '', 'Italic', 'Narrow', 'Narrow Bold', 'Narrow Italic', 'Regular' |
| colour | Optional[string] | yes | required |  |  | enum: '', 'black', 'process_blue', 'red' |
| size | Optional[number] | yes | required |  |  |  |
| placement | Optional[string] | yes | required |  |  | enum: '', 'AL', 'BL', 'OL' |
| offset | Optional[number] | yes | required |  |  |  |
| textanchor | Optional[string] | yes | required |  |  | enum: '', 'centre', 'left', 'left ', 'left 0.62', 'right' |
| labelanchor | Optional[number] | yes | required |  |  |  |
| charplace | Optional[string] | yes | required |  |  | enum: '', 'CharactersAtVertices', 'StretchCharacterSpacingToFit', 'StretchWordSpacingToFit' |
| chardistance | Optional[number] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50CoastlineIsland

Derived polygon Union of coastline and island

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'coastline' |
| elevation | Optional[integer] | yes | required |  |  |  |
| name | Optional[string] | yes | required |  |  |  |
| group_name | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50DmsGrid

Generated model for Nztopo50DmsGrid.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| direction | string | yes | required |  |  | enum: 'x', 'y' |
| value | number | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50Grid

Generated model for Nztopo50Grid.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| direction | string | yes | required |  |  | enum: 'x', 'y' |
| value | number | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50IceContour

Generated model for Nztopo50IceContour.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'contour_ice' |
| elevation | Optional[integer] | yes | required |  |  |  |
| definition | Optional[string] | yes | required |  |  |  |
| designation | Optional[string] | yes | required |  |  | enum: 'supplementary' |
| formation | Optional[string] | yes | required |  |  | enum: 'depression' |
| theme | string | yes | required |  |  | enum: 'relief' |
| contour_id | string | yes | required |  | UUID for the intersecting contour feature. |  |
| landcover_id | string | yes | required |  | UUID for the intersecting landcover feature. |  |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50MapSheet

Generated model for Nztopo50MapSheet.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'nztopo50_map_sheet' |
| sheet_code | string | yes | required |  |  |  |
| sheet_name | string | yes | required |  |  |  |
| origin_x | number | yes | required |  |  |  |
| origin_y | number | yes | required |  |  |  |
| example_point_id | string | yes | required |  | id (UUID) of the example feature. |  |
| published_version | string | yes | required |  |  |  |
| published_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50RockLine

Generated model for Nztopo50RockLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| marine_id | string | yes | required |  | UUID of the source `marine` rock polygon this boundary line was derived from. |  |
| type | string | yes | required |  |  | enum: 'rock' |
| name | Optional[string] | yes | required |  |  |  |
| sub_type | Optional[string] | yes | required |  |  | enum: 'coral', 'limestone', 'pumice', 'rock' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Nztopo50SeaPolygon

Derived sea (moana) polygons for the water layer.

The land polygons (coastline and island) are inverted and sliced by Web
Mercator quadkey tiles so no single large polygon exists.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  |  |  |
| type | string | yes | required |  |  | enum: 'moana' |
| quadkey | string | yes | required |  | Web Mercator quadkey of the tile this sea polygon was sliced to. |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## PlacePoint

Generated model for PlacePoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cave', 'grave', 'historic_site', 'monument', 'pa' |
| subtype | Optional[string] | yes | required |  |  | enum: 'coral', 'limestone', 'pumice', 'rock' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landuse' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## RailwayLine

All mainline railway lines are held in the Topo50 data and shown on the Topo50 printed maps.
Where a railway line is located close to a road, the line held in the data and shown on the printed map
may be offset from the road sufficient that the two symbols are recognisable at 1:50,000.

Multiple sidings may be held in the data and shown on the printed maps as a single feature

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'railway' |
| subtype | Optional[string] | yes | required |  |  | enum: 'siding' |
| track_type | Optional[string] | yes | required |  |  | enum: 'single', 'multiple' |
| vehicle_type | Optional[string] | yes | required |  |  | enum: 'train', 'tram', 'rail_cart', 'cablecar' |
| status | Optional[string] | yes | required |  |  | enum: 'disused' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## RailwayPoint

Generated model for RailwayPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'station' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Relief

Generated model for Relief.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'waterfall' |
| name | Optional[string] | yes | required |  |  |  |
| height | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'relief' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## ReliefLine

Generated model for ReliefLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cliff_edge', 'cutting_edge', 'embankment', 'rapid', 'slip_edge', 'waterfall', 'waterfall_edge', 'seawall' |
| subtype | Optional[string] | yes | required |  |  | enum: 'causeway', 'stopbank' |
| name | Optional[string] | yes | required |  |  |  |
| height | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'relief' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## ReliefPoint

Generated model for ReliefPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'height', 'saddle', 'sinkhole', 'waterfall' |
| name | Optional[string] | yes | required |  |  |  |
| elevation | Optional[integer] | yes | required |  |  |  |
| height | Optional[number] | yes | required |  |  |  |
| orientation | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'relief' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## ResidentialArea

Generated model for ResidentialArea.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'residential_area' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landuse' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## RoadLine

Generated model for RoadLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'road' |
| hierarchy | Optional[string] | yes | required |  |  |  |
| status | Optional[string] | yes | required |  |  | enum: 'under_construction', 'closed' |
| name | Optional[string] | yes | required |  |  |  |
| highway_number | Optional[string] | yes | required |  |  |  |
| lane_count | Optional[integer] | yes | required |  |  |  |
| surface | Optional[string] | yes | required |  |  | enum: 'metalled', 'unmetalled', 'sealed' |
| way_count | Optional[string] | yes | required |  |  | enum: 'one_way' |
| width_indicator | Optional[string] | yes | required |  |  | enum: 'w' |
| road_access | Optional[string] | yes | required |  |  | enum: 'mp' |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## RockOutcrop

Generated model for RockOutcrop.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'rock_outcrop' |
| orientation | number | yes | required |  |  |  |
| elevation | integer | yes | required |  |  |  |
| subtype | string | yes | required |  |  | enum: 'small_rock_outcrop', 'large_rock_outcrop', 'large_boulder' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Runway

Generated model for Runway.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'runway' |
| subtype | Optional[string] | yes | required |  |  | enum: 'aerodrome', 'airport', 'airstrip' |
| status | Optional[string] | yes | required |  |  | enum: 'disused' |
| surface | Optional[string] | yes | required |  |  | enum: 'grass', 'sealed' |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Structure

Generated model for Structure.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'dry_dock', 'fish_farm', 'marine_farm', 'reservoir', 'siphon', 'tank' |
| subtype | Optional[string] | yes | required |  |  | enum: 'building', 'coal', 'concrete', 'disused', 'fuel', 'historic', 'jetty', 'lake', 'land', 'lighthouse', 'lime', 'locked', 'plane', 'power_generation', 'rock', 'sea', 'ship', 'surge_chamber', 'tunnel_ventilation', 'uncovered', 'water' |
| lid_type | Optional[string] | yes | required |  |  | enum: 'covered' |
| tank_type | Optional[string] | yes | required |  |  | enum: 'uncovered', 'water', 'surge_chamber' |
| species | Optional[string] | yes | required |  |  | enum: 'mussels', 'salmon' |
| status | Optional[string] | yes | required |  |  | enum: 'closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'structures' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## StructureLine

Generated model for StructureLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'boatramp', 'boom', 'breakwater', 'cableway_industrial', 'cableway_people', 'dam', 'flume', 'ladder', 'marine_farm', 'ski_lift', 'ski_tow', 'slipway', 'spillway_edge', 'walkwire', 'water_race', 'weir', 'wharf', 'wharf_edge' |
| subtype | Optional[string] | yes | required |  |  | enum: 'building', 'coal', 'concrete', 'disused', 'fuel', 'historic', 'jetty', 'lake', 'land', 'lighthouse', 'lime', 'locked', 'plane', 'power_generation', 'rock', 'sea', 'ship', 'surge_chamber', 'tunnel_ventilation', 'uncovered', 'water' |
| species | Optional[string] | yes | required |  |  | enum: 'mussels', 'salmon' |
| status | Optional[string] | yes | required |  |  | enum: 'closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'structures' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## StructurePoint

Generated model for StructurePoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'beacon', 'bivouac', 'buoy', 'chimney', 'dredge', 'flare', 'floodgate', 'gate', 'ladder', 'lighthouse', 'mast', 'radar_dome', 'redoubt', 'satellite_station', 'shaft', 'siphon', 'stockyard', 'tank', 'tower', 'well', 'windmill', 'wreck' |
| subtype | Optional[string] | yes | required |  |  | enum: 'building', 'coal', 'concrete', 'disused', 'fuel', 'historic', 'jetty', 'lake', 'land', 'lighthouse', 'lime', 'locked', 'plane', 'power_generation', 'rock', 'sea', 'ship', 'surge_chamber', 'tunnel_ventilation', 'uncovered', 'water' |
| tank_type | Optional[string] | yes | required |  |  | enum: 'uncovered', 'water', 'surge_chamber' |
| status | Optional[string] | yes | required |  |  | enum: 'closed', 'dangerous', 'derelict', 'disused', 'historic', 'locked', 'old', 'private', 'remains', 'ruins', 'under_construction' |
| name | Optional[string] | yes | required |  |  |  |
| height | Optional[number] | yes | required |  |  |  |
| orientation | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'structures' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## TrackLine

Generated model for TrackLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'track' |
| subtype | Optional[string] | yes | required |  |  | enum: 'cycle_only', 'foot', 'vehicle' |
| track_type | Optional[string] | yes | required |  |  | enum: 'connector', 'multiple', 'route', 'single' |
| status | Optional[string] | yes | required |  |  | enum: 'closed' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## TransportPoint

Generated model for TransportPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'ford' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## TrigPoint

Generated model for TrigPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'trig' |
| trig_type | Optional[string] | yes | required |  |  | enum: 'beaconed' |
| name | Optional[string] | yes | required |  |  |  |
| code | Optional[string] | yes | required |  |  |  |
| elevation | Optional[integer] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'relief' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## TunnelLine

Generated model for TunnelLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'cablecar', 'foot_traffic', 'livestock', 'river', 'train', 'tram', 'vehicle' |
| subtype | Optional[string] | yes | required |  |  | enum: 'livestock' |
| construction_type | Optional[string] | yes | required |  |  | enum: 'natural', 'manmade' |
| status | Optional[string] | yes | required |  |  | enum: 'closed', 'historic', 'disused', 'derelict', 'under_construction' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'transport' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## UtilityLine

Generated model for UtilityLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'pipeline' |
| subtype | Optional[string] | yes | required |  |  | enum: 'ironsand', 'sewage', 'steam', 'water' |
| support_type | Optional[string] | yes | required |  |  | enum: 'pole', 'pylon' |
| status | Optional[string] | yes | required |  |  |  |
| visibility | Optional[string] | yes | required |  |  | enum: 'underground' |
| theme | string | yes | required |  |  | enum: 'utility' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## UtilityPoint

Generated model for UtilityPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'gas_valve', 'geo_bore', 'pylon' |
| orientation | Optional[number] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'utility' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Vegetation

Generated model for Vegetation.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'exotic' |
| subtype | Optional[string] | yes | required |  |  | enum: 'coniferous', 'non-coniferous' |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## VegetationLine

Generated model for VegetationLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'shelter_belt' |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## VegetationPoint

Generated model for VegetationPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'tree' |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## Water

Generated model for Water.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'canal', 'drain', 'lagoon', 'lake', 'pond', 'river', 'soakhole', 'spring', 'waterfall' |
| subtype | Optional[string] | yes | required |  |  | enum: 'evaporation', 'hydro_electric', 'ice_skating', 'oil', 'oxidation', 'reservoir', 'settling', 'sewage', 'sewage_treatment', 'sludge' |
| name | Optional[string] | yes | required |  |  |  |
| group_name | Optional[string] | yes | required |  |  |  |
| height | Optional[number] | yes | required |  |  |  |
| elevation | Optional[integer] | yes | required |  |  |  |
| perennial | Optional[string] | yes | required |  |  | enum: 'dry', 'seasonal' |
| temperature_indicator | Optional[string] | yes | required |  |  | enum: 'cold', 'hot' |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## WaterLine

Generated model for WaterLine.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'canal', 'drain', 'lagoon', 'lake', 'pond', 'river', 'soakhole', 'spring', 'waterfall' |
| name | Optional[string] | yes | required |  |  |  |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## WaterPoint

Generated model for WaterPoint.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| id | string | yes | required |  | UUIDv7 of the feature |  |
| created_at | datetime | yes | required |  |  |  |
| updated_at | datetime | yes | required |  |  |  |
| t50_fid | Optional[integer] | yes | required |  | Reference topo50 feature ID.  Will be null if the feature is new and has not been published in a Topo50 edition. |  |
| type | string | yes | required |  |  | enum: 'canal', 'drain', 'lagoon', 'lake', 'pond', 'river', 'soakhole', 'spring', 'waterfall' |
| name | Optional[string] | yes | required |  |  |  |
| height | Optional[number] | yes | required |  |  |  |
| orientation | Optional[number] | yes | required |  |  |  |
| temperature_indicator | Optional[string] | yes | required |  |  | enum: 'cold', 'hot' |
| theme | string | yes | required |  |  | enum: 'landcover' |
| metadata | Optional[string] | yes | required |  |  |  |
| geometry | unknown | yes | required |  | GeoParquet 1.1 covering geometry struct. |  |
| bbox | Optional[BBox] | no | None |  | GeoParquet 1.1 covering bbox struct. |  |

## BBox

GeoParquet 1.1 covering bbox struct.

| Field | Type | Required | Default | Max Length | Description | Extra |
| --- | --- | --- | --- | --- | --- | --- |
| xmin | number | yes | required |  |  |  |
| ymin | number | yes | required |  |  |  |
| xmax | number | yes | required |  |  |  |
| ymax | number | yes | required |  |  |  |
