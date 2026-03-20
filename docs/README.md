# Topographic System

The topographic system is a collection of components that are used to create New Zealand's Topo50 https://www.linz.govt.nz/products-services/maps/new-zealand-topographic-maps

## Components

- [linz/topographic-system](https://github.com/linz/topographic-system) - Source code and scripts to control the entire process
- [linz/topographic-qgis](https://github.com/linz/topographic-qgis) - QGIS project files and symbology for editing and creating topographic data and maps

## Topographic Datasets

topographic data is stored as kart repos roughly broken down into a similar groups, some large datasets (contours) are in seperate repositories due to performance impacts of their size

- [linz/topographic-data](https://github.com/linz/topographic-data) - Topographic data eg `water` or `airport`
- [linz/topographic-product-data](https://github.com/linz/topographic-product-data) - Product specific datasets (eg `nz_topo50_map_sheet`)
- [linz/topographic-contour-data](https://github.com/linz/topographic-contour-data) - Topo50 Contour lines