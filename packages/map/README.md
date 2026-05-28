# Map Production

Utilities to run a map production flow inside of a container

## Map Export

Exports are made by first creating a map production run with `map prepare`, once the STAC documents are created the assets can be exported with `map export`. Both of these commands require `QGIS` to be installed and is easier to run inside of the prepared container.

```shell
# Build the map container
docker build -f packages/map/Dockerfile -t map .

# Prepare a export for mapsheet BQ32
docker run -it -v $PWD:/working map prepare \
  --project https://d1jzh93b1t1cv.cloudfront.net/qgis/nztopo50/latest/nztopo50.json \
  --output /working/output/ \
  --cache /working/.cache/ \
  BQ32 BQ33

# export mapsheet BQ32
docker run -it -v $PWD:/working map export \
   --cache /working/.cache/ \
   /working/output/nztopo50/BQ33.json
```

## Building container

Build docker containr from the git repository root directory to include python project [qgis](../qgis/README.md).

```shell
docker build -f packages/map/Dockerfile -t map .
```

## Visual Diff Cli

Run it in the container

```shell
docker run -it --rm -v ~/.aws:/root/.aws:ro -v $PWD}:/working -e AWS_PROFILE=li-topo-maps-nonprod map visual-diff --project s3://linz-topography-nonprod/qgis/latest/nztopo50map/nz-topo50-map.json --output /working/output
```

## Deploy QGIS Project Cli

### Write files locally

- Dry run

  ```shell
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target tmp/ --tag abc123
  ```

- Create files

  ```shell
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target tmp/ --tag abc123 --commit
  ```

### Write files to S3

- Dry run

  ```shell
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target s3://linz-topography-nonprod/qgis/ --tag abc123
  ```

- Create files

  ```shell
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target s3://linz-topography-nonprod/qgis/ --tag latest --commit
  ```

## Debug

### Debug in container

```shell
docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -v ${PWD}:${PWD} -e AWS_PROFILE=li-topo-maps-nonprod map
```

### Run cli in container

```shell
node src/index.ts produce --project s3://linz-topography-nonprod/qgis/latest/nztopo50map/nz-topo50-map.json --output output --format tiff --dpi 200 AW26 AW27
```

### Debug python in container

```shell
docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -v $PWD/packages/qgis:/app/qgis -e AWS_PROFILE=li-topo-maps-nonprod map
```

```shell
python qgis/src/qgis_export.py tmp/01KB3R0CJNK675G6H4ENCXCBMP/topo50-map.qgz tmp/01KB3R0CJNK675G6H4ENCXCBMP/output/ tiff 300 AW26ls
```
