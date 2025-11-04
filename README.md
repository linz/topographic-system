# Topographic-System

## Build
```
npm i
docker build -t cli .
```

## Run
1. [Get the test parquet data](https://linzsrm.sharepoint.com/sites/Topography/Shared%20Documents/Forms/AllItems.aspx?id=%2Fsites%2FTopography%2FShared%20Documents%2FFuture%20Topo%20Maps%2FData%20management%2Fqgis%2Deditor%2Frelease62%5Fparquet%2Ezip&parent=%2Fsites%2FTopography%2FShared%20Documents%2FFuture%20Topo%20Maps%2FData%20management%2Fqgis%2Deditor&p=true&ga=1)

2. QGIS project `git clone git@github.com:linz/topographic-qgis.git`

3. Create a location for the pdf outputs `mkdir out`

4. Run it in the container and mount the parquet data, qgis project and the output folder `docker run -it --rm -v ../topographic-qgis/map-series/topo50map/topo50-map.qgz:/data/topo50-map.qgz -v ../release62_parquet/2025-02-05:/data/ -v ../out:/out cli produce`

## Debug
Drop into the container with the current folder mounted
```
docker run -it --rm --entrypoint /bin/bash -v ./:/app -v ../topographic-qgis/map-series/topo50map/topo50-map.qgz:/data/topo50-map.qgz -v ../release62_parquet/2025-02-05:/data/ -v ../out:/out cli
node src/index.ts produce
```