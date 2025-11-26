# Map Production

## Build

```
docker build -t map .
```

## Run

Run it in the container `docker run -it --rm -v ~/.aws:/root/.aws:ro -e AWS_PROFILE=li-topo-maps-nonprod map produce --source s3://linz-topography-nonprod/topo/test/2025-02-05/ --project s3://linz-topography-nonprod/carto/test/latest/topo50-map.qgz --output s3://linz-topography-scratch-nonprod/topo50map/test/ --format tif --dpi 200 AW26 AW27`

## Debug

```
docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -e AWS_PROFILE=li-topo-maps-nonprod map
node src/index.ts produce --source s3://linz-topography-nonprod/topo/test/2025-02-05/ --project s3://linz-topography-nonprod/carto/test/latest/topo50-map.qgz --output output --format tif --dpi 200 AW26 AW27
```
