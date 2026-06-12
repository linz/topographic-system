2/06/2026 

Shapefile field names are restricted in length - these are renamed to longer versions. These are in the LDS data not matter the export format.

Data going into repo topographic-data is converted to EPSG:4167 New Zealand Geodetic Datum 2000 (NZGD2000). It is a geographic (lat/long) coordinate system based on the GRS80 ellipsoid.

A type fields is added to all layers - the feature type is based on the shapefile name - loaded from layers_info.csv control file.

An id (formally topo_id) field (UUID/GUID) is added and unique value assigned. 

Metadata fields are added to all layers. The create_date->created_at and update_date->updated_at - field renames.

Name changes - use and type fields are renamed based on layer name or consistent naming for example road_use, road_type. The field on the right will no longer exist.

update_dict = {
            (schema_name, "structure_point"): [
                ("structure_use", "shaft_use"),
                ("structure_type", "tank_type"),
                ("material", "materials"),
            ],
            (schema_name, "structure_line"): [("status", "dam_status")],
            (schema_name, "structure"): [
                ("species", "species_cultivated"),
                ("lid_type", "reservoir_lid_type"),
                ("structure_type", "tank_type"),
            ],
            (schema_name, "road_line"): [("highway_number", "highway_numb")],
            (schema_name, "water"): [
                ("water_use", "pond_use"),
            ],
  }

If shapefile has "ESRI_OID" it is dropped.

Data columns are re-ordered to ensure geometry is the last column.

Islands - pre-loading step - Island are intersected with a created sea polygon (coastline and outer box) to identify islands in the sea versus “inland”. A new field is added to the dataset called location where 1 = a sea-based island and 0 is inland. - this is being dropped or new requirements required. 

Road_line has a future field added called name_id and width_indicator. These come from data exported directly from LAMPS and using lookup based on the t50_fid.

vegetation_points (formally tree_locations) - has an unrequired field name - this is dropped. Not only 3 trees have a name in the source - this is dropped but record for LDS recreation process.

values are fixed or realigned into common fields / give default values

tunnel_line

value ‘ivestock’ updated to 'livestock'

tunnel_use updated to vehicle where use2 = vehicle

tunnel_use2 updated to livestock where use2 = vehicle

"trig_point", "trig_type", "'beaconed'"

"road_line", "way_count", "'one way'", "way_count ='1'"

 "road_line", "road_access", "'mp'", "road_access ='m'"

"utility_line (formally physical_Infrastructure_line)", "support_type", "'pole'","type ='telephone'"

A name field is added to these layers

            "utility_point (previous name - physical_infrastructure_point)",
            "utility_line (previous name - physical_infrastructure_line",
            "structure",
            "ferry_crossing",

Version 0.2 of model - changes 
        "vegetation", - dropped
        "landcover", - now there because waterfall name
        "landcover_line", - dropped

Where null defines a value - set a value instead

        update_dict = {
            f"{schema_name}.runway": [("surface", "'grass'", "")],
            f"{schema_name}.vegetation": [
                ("species", "'coniferous'", "AND type = 'exotic'")],
            f"{schema_name}.railway_line": [("vehicle_type", "'train'", "")],
            # Add more entries as needed - should be pre-existing
        }



Added / Modfield Fields 

trig_points

    self.table_modifer.add_column(
        f"{self.schema_name}.trig_point", "code", "VARCHAR(20)"
    )
    self.table_modifer.update_value_by_column(
        self.schema_name, "trig_point", "code", "name"
    )
    self.table_modifer.update_value_by_column(
        self.schema_name, "trig_point", "name", "null"
    )

vegetation

    self.table_modifer.add_column(
        f"{self.schema_name}.vegetation", "subtype", "VARCHAR(50)"
    )
    self.table_modifer.update_value_by_column(
        self.schema_name, "vegetation", "subtype", "species"
    )
    self.table_modifer.update_value_by_column(
        self.schema_name, "vegetation", "species", "null"
    )



landcover

   self.table_modifer.add_column(
        f"{self.schema_name}.landcover", "subtype", "VARCHAR(50)"
    )



road_line

    self.table_modifer.add_column(
        f"{self.schema_name}.road_line", "hierarchy", "VARCHAR(50)"
    )

    These fields are populated from lookup table created from LAMPS data dump of toads.
    self.table_modifer.add_column(
        f"{self.schema_name}.road_line", "width_indicator", "VARCHAR(5)"
    )
    self.table_modifer.add_column(
        f"{self.schema_name}.road_line", "name_id", "BIGINT"
    )



railway_line

    self.table_modifer.add_column(
        f"{self.schema_name}.railway_line", "route", "VARCHAR(30)"
    )
    self.table_modifer.add_column(
        f"{self.schema_name}.railway_line", "route2", "VARCHAR(30)"
    )
    self.table_modifer.add_column(
        f"{self.schema_name}.railway_line", "route3", "VARCHAR(30)"
    )



coastline

    self.table_modifer.add_column(
        f"{self.schema_name}.coastline", "coastline_type", "VARCHAR(50)"
    )



hierarchy
   self.table_modifer.add_column(
        f"{self.schema_name}.road_line", "hierarchy", "VARCHAR(25)"
    )

    self.table_modifer.add_column(
        f"{self.schema_name}.water_line", "hierarchy", "VARCHAR(25)"
    )
    self.table_modifer.add_column(
        f"{self.schema_name}.water", "hierarchy", "VARCHAR(25)"
    )

other changes

    self.table_modifer.rename_columns(
        self.schema_name, "contour", "designated", "designation"
    )

    self.table_modifer.rename_columns(
        self.schema_name, "landuse", "track_type", "landuse_type"
    )
    self.table_modifer.update_value_by_column(
        self.schema_name, "landuse", "landuse_type", "visibility"
    )
    self.table_modifer.drop_column(self.schema_name, "landuse", "visibility")
    self.table_modifer.rename_columns(
        self.schema_name, "landuse_line", "track_type", "landuse_type"
    )
    self.table_modifer.rename_columns(
        self.schema_name, "place_point", "visibility", "place_type"
    )



topographic-product-data retains original projections 

nz_topo50_map_sheet NZTM2000 (EPSG:2193)

Carto_text NZTM2000 (EPSG:2193)

grid 1 - WGS84

grid 2 - NZTM2000 (EPSG:2193)

