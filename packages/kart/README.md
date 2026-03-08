# Kart wrapper

## Table of Contents

- [Bundle and Build](#bundle-and-build)
- [Clone cli](#clone-cli)
- [Diff cli](#diff-cli)
- [Export cli](#export-cli)
- [To Parquet cli](#to-parquet-cli)
- [PR Comment cli](#pr-comment-cli)
- [Validate cli](#validate-cli)
- [Contour with Landcover cli](#contour-with-landcover-cli)
- [Version cli](#version-cli)
- [Sample GitHub Actions Workflow](#sample-github-actions-workflow)

## Bundle and Build

Bundle the package before building the docker image. Run both commands at root directory.

```
npx lerna run bundle --stream
docker build -f packages/kart/Dockerfile -t kart .
```

## Clone cli

Pass the GITHUB_TOKEN env variable and mount a local folder to clone a kart repo.
The repository can be specified as a short path (e.g. `linz/topographic-data`) or a full URL (e.g. `https://github.com/linz/topographic-data.git`).
If `--ref` is not specified, defaults to `master`.

```
docker run -it --rm -e GITHUB_TOKEN -v /tmp/docker:/tmp kart clone linz/topographic-data
```

**Clone specific branch or commit:**

```
docker run -it --rm -e GITHUB_TOKEN -v /tmp/docker:/tmp kart clone https://github.com/linz/topographic-data.git --ref feature-branch
```

## Diff cli

Run diff commands on a cloned kart repository to compare changes between commits or branches.

**Default diff (master..FETCH_HEAD):**

```
docker run -it --rm -v /tmp/docker:/tmp kart diff
```

**Custom diff range:**

```
docker run -it --rm -v /tmp/docker:/tmp kart diff commit1..commit2
```

## Export cli

Mount a local folder containing the cloned repo to export geopackage.
When no arguments are provided, this will export all datasets from the cloned repository.

**All datasets:**

```
docker run -it --rm -v /tmp/docker:/tmp kart export
```

**Only datasets with changes:**

```
docker run -it --rm -v /tmp/docker:/tmp kart export --changed-datasets-only
```

**Specific dataset(s):**

```
docker run -it --rm -v /tmp/docker:/tmp kart export marine airport
```

**Export from specific commit:**

```
docker run -it --rm -v /tmp/docker:/tmp kart export --ref commit-sha
```

## To Parquet cli

Convert .gpkg files to geoparquet.
When no arguments are provided, this will convert all datasets from the `./export` folder.
Output parquet files will be saved to `./parquet` folder.
Converted parquet files are uploaded to S3, so AWS credentials and the `ENVIRONMENT` variable are required.

**All datasets:**

```
docker run -it --rm -e ENVIRONMENT="nonprod" -e AWS_PROFILE -e AWS_REGION=ap-southeast-2 -v /tmp/docker:/tmp -v ~/.aws:/root/.aws:ro kart to-parquet
```

**Specific dataset(s):**

```
docker run -it --rm -e ENVIRONMENT="nonprod" -e AWS_PROFILE -e AWS_REGION=ap-southeast-2 -v /tmp/docker:/tmp -v ~/.aws:/root/.aws:ro kart to-parquet export/marine.gpkg export/airport.gpkg
```

## PR Comment cli

Add or update a pull request comment with diff results.

**Auto-detect PR and repo:**

```
docker run -it --rm -e GITHUB_TOKEN -v /tmp/docker:/tmp kart pr-comment
```

**Specify PR number and repo:**

```
docker run -it --rm -e GITHUB_TOKEN -v /tmp/docker:/tmp kart pr-comment --pr 123 --repo linz/topographic-data
```

**Custom comment body file:**

```
docker run -it --rm -e GITHUB_TOKEN -v /tmp/docker:/tmp kart pr-comment custom_summary.md
```

## Validate cli

Run topographic data validation against parquet files.
By default, validates all parquet files in `/tmp/kart/parquet/*.parquet` using the built-in config.
Validation results are uploaded to S3, so AWS credentials and the `ENVIRONMENT` variable are required.

See [data-review.yml](../../.github/workflows/data-review.yml) for a usage example.
The workflow above can be called by a data repository as follows:
```yaml
name: PR Review

on: pull_request

jobs:
  data-review:
    uses: linz/topographic-system/.github/workflows/data-review.yml@master
```

**With defaults:**
```
docker run -it --rm -e ENVIRONMENT="nonprod" -e AWS_PROFILE -e AWS_REGION=ap-southeast-2 -v /tmp/docker:/tmp -v ~/.aws:/root/.aws:ro kart validate
```

**Custom db path and output directory:**
```
docker run -it --rm -e ENVIRONMENT="nonprod" -e AWS_PROFILE -e AWS_REGION=ap-southeast-2 -v /tmp/docker:/tmp -v ~/.aws:/root/.aws:ro kart validate --db-path /tmp/kart/parquet/files.parquet --output-dir /tmp/kart/validation/
```

**With bounding box filter and verbose output:**
```
docker run -it --rm -e ENVIRONMENT="nonprod" -e AWS_PROFILE -e AWS_REGION=ap-southeast-2 -v /tmp/docker:/tmp -v ~/.aws:/root/.aws:ro kart validate --export-parquet --verbose --bbox 174.711,-41.349,175.04,-41.17  --output-dir /tmp/kart/validation/
```

## Contour with Landcover cli

Enrich contour data with landcover information, also known as create ice-contours. Requires paths to contour and landcover parquet files and an output path. This is generally invoked through Argo Workflows.

```
docker run -it --rm -v /tmp/docker:/tmp kart contour-with-landcover --contour ./contour.parquet --landcover ./landcover.parquet --output ./output.parquet
```

## Version cli

Output the kart version.
```
docker run -it --rm -v /tmp/docker:/tmp kart version
```

## Sample GitHub Actions Workflow

See [data-review.yml](../../.github/workflows/data-review.yml) for a complete example of how to use the kart CLI commands in a GitHub Actions workflow for pull request data reviews.

That workflow will:
1. Check the kart version for debugging
2. Clone the repository with the PR branch
3. Generate a diff comparing the PR changes against master
4. Post a comment on the PR with the diff results
5. Export all datasets with changes as geopackages
6. Convert all geopackages to parquet files, generate stac and upload to s3
7. Validate the datasets and upload results to s3

The workflow runs on every pull request and provides automated quality checks and diff visualization for topographic data changes.
