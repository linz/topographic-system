## End to End testing

Using the two build docker containers, 

- Clone a kart repository and export it to parquet
- Deploy a testing [topo-test.qgs](./topo-test.qgs) QGIS project file
- Create a map production run
- Create the output map files

### Usage

```
node index.ts \
  --container-kart=ghcr.io/linz/topographic-system/kart:latest \
  --container-map=ghcr.io/linz/topographic-system/map:latest
```