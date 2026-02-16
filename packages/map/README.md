# Map Production

## Build

Build docker at root directory to include python project.

```
docker build -f packages/map/Dockerfile -t map .
```

## Produce cli

Run it in the container

```
docker run -it --rm -v ~/.aws:/root/.aws:ro -v ${PWD}:${PWD} -e AWS_PROFILE=li-topo-maps-nonprod map produce --project s3://linz-topography-nonprod/qgis/latest/nztopo50map/nz-topo50-map.json --output $PWD/output --format tiff --dpi 200 AW26 AW27
```

## List Map Sheets Cli

```
node src/index.ts list-mapsheets --project s3://linz-topography-nonprod/qgis/latest/nztopo50map/nz-topo50-map.json --output output.json
```

## Deploy QGIS Project Cli

### Write files locally

- Dry run

  ```
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target tmp/ --tag abc123
  ```

- Create files

  ```
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target tmp/ --tag abc123 --commit
  ```

### Write files to S3

- Dry run

  ```
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target s3://linz-topography-nonprod/qgis/ --tag abc123
  ```

- Create files

  ```
  node src/index.ts deploy --project {path_to}/topographic-qgis/map-series/ --target s3://linz-topography-nonprod/qgis/ --tag latest --commit
  ```

## Debug

### Debug in container

```
docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -e AWS_PROFILE=li-topo-maps-nonprod map
```

### Run cli in container

```
node src/index.ts produce --project s3://linz-topography-nonprod/qgis/latest/nztopo50map/nz-topo50-map.json --output output --format tiff --dpi 200 AW26 AW27
```

### Debug python in container

```
docker run -it --rm --entrypoint /bin/bash -v ~/.aws:/root/.aws:ro -v $PWD/packages/qgis:/app/qgis -e AWS_PROFILE=li-topo-maps-nonprod map
```

```
python qgis/src/qgis_export.py tmp/01KB3R0CJNK675G6H4ENCXCBMP/topo50-map.qgz tmp/01KB3R0CJNK675G6H4ENCXCBMP/output/ tiff 300 AW26ls
```
