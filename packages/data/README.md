# Data

## Build

```
docker build -f packages/data/Dockerfile -t data .
```

## Ice contours cli

```
docker run -it --rm -v ~/.aws:/root/.aws:ro -e AWS_PROFILE=li-topo-maps-nonprod data ice-contours --contour s3://linz-topography-scratch-nonprod/contour.parquet --landcover s3://linz-topography-scratch-nonprod/landcover.parquet --output s3://linz-topography-scratch-nonprod/nz_topo50_contour_inter.parquet
```