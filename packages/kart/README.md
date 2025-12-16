# Kart Package

This package provides CLI tools and Python utilities for working with Kart repositories, including cloning and exporting layers in parallel.

## Build

To build the Docker image for the Kart package, run:

```
docker build -f packages/kart/Dockerfile -t kart .
```

## Kart CLI Usage

The Kart CLI provides subcommands for cloning and exporting layers from a Kart repository.

### Clone a Repository

Clone a repository and optionally checkout a specific commit SHA:

```
docker run -it --rm -e GITHUB_TOKEN=$GITHUB_TOKEN -v /tmp/docker-out:/tmp kart clone --repository  https://github.com/linz/topographic-data.git --sha master
```
- `--repository` (required): Full URL of the repository to clone
- `--sha` (optional): Commit SHA to check out (default: master)

### Export Layers

Export one or more layers from a repository, optionally in parallel:

```
docker run -it --rm -v kart export --repository_path topographic-data [--sha <COMMIT_SHA>] [--layers layer1 layer2 ...] [--num-procs N]
```
- `--repository` (required): Full URL of the repository to export from
- `--sha` (optional): Commit SHA to export (default: latest)
- `--layers` (optional): List of layers to export (default: all layers)
- `--num-procs` (optional): Number of parallel export processes (default: number of CPUs)

## Data Output and Host Access

All cloned repositories and exported data are written to `/tmp/data` inside the container. To make this data available on your host, mount a host directory (e.g., `/tmp/docker-out`) to `/tmp` or `/tmp/data`:

```
docker run -it --rm \
  -v /tmp/docker-out:/tmp \
  ...other options... \
  kart clone --repository <REPO_URL>
```

After the container exits, your data will be available in `/tmp/docker-out` on your host.


## Development

- Python source code is in `packages/kart/src/`.
- Main CLI entrypoint: `packages/kart/src/cli.py`
- Kart command utilities: `packages/kart/src/kart/kart_commands.py`

