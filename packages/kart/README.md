# Kart wrapper

## Bundle and Build

Bundle the package before building the docker image. Run both commands at root directory.

```
npx lerna run bundle --stream
docker build -f packages/kart/Dockerfile -t kart ./packages/kart/
```

## Clone cli

Pass the GITHUB_TOKEN env variable and mount a local folder to clone a kart repo.

```
docker run -it --rm -e GITHUB_TOKEN="GITHUB_TOKEN" -v /tmp/docker:/tmp kart clone linz/topographic-data
```

## Export cli

Mount a local folder to export geopackage from a kart repository.
When no datasets are listed, this will export all datasets from the cloned repo.

**All datasets:**
```
docker run -it --rm -v /tmp/docker:/tmp kart export
```

**Specific dataset(s):**
```
docker run -it --rm -v /tmp/docker:/tmp kart export marine airport
```

## Version cli

Output the kart version.
```
docker run -it --rm -v /tmp/docker:/tmp kart version
```
