# Data Sources

Data comes from various sources


## Topographic Data
Core topographic data loaded into topographic-data

This mainly comes from shapefiles that have gone through the LDS load preparation steps (sets the correct t50_fid and joins data across map sheets).

All feature types are created from this data with the following exceptions:

**road_cl.shp** - this is exported directly from LAMPS. 

Macrons need to be checked.

This replaces the LDS shp version.

The road_cl.cpg file should be deleted


**TODO: river_cl.shp** - we want the name field which is dropped from LDS load.

**Currently Manual - Sea Polygon** - used in pre-precess step to update island_poly location (sea/land)
sea_polygon \ sea_coastline.shp is create using ArcGIS Pro manually. Creating a box larger than the map sheets; converting coastline to polygons and cutting them out of the box polygon. Leaving a sea area. Note this does not include islands.

Instructions: TBC

 
 ## Topographic Contours
 
 **topographic-contour-data** - the contour data LDS shp file should be 2 files. If only one this usually only covers one island.

 The current process use the contours downloaded from LDS instance as a GPKG database - lds-nz-contours-topo-150k-GPKG


## Product (carto) Data

**Topo50_carto_text_2020_09** is export directly from LAMPS

Once loaded into POSTGIS step addition processing is run to add QGIS fields and update new values. Becomes nz_topo50_carto_text


**dms_grid_3.gpkg** - created by North Road - used as grid on the topo50 map. Becomes nz_topo50_grid.

**grid.gpkg** - created by North Road - used as grid on the topo50 map. Becomes nz_topo50_dms_grid.

