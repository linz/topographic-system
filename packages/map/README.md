# Map Production

## Build

```
docker build -t map .
```

## Run

Run it in the container `docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -v ~/src/topographic-qgis/map-series/topo50map/topo50-map.qgz:/tmp/test/topo50-map.qgz -e AWS_PROFILE=li-topo-maps-nonprod map produce --source s3://linz-topography-nonprod/topo/test/2025-02-05/ --project s3://linz-topography-nonprod/carto/test/latest/topo50-map.qgz --output s3://linz-topography-scratch-nonprod/topo50map/test/`

## Debug

```
docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -e AWS_PROFILE=li-topo-maps-nonprod map
node app/src/index.ts produce --source s3://linz-topography-nonprod/topo/test/2025-02-05/ --project s3://linz-topography-nonprod/carto/test/latest/topo50-map.qgz --output output
```
