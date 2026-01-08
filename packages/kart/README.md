# Kart wrapper

## Table of Contents

- [Bundle and Build](#bundle-and-build)
- [Clone cli](#clone-cli)
- [Diff cli](#diff-cli)
- [Export cli](#export-cli)
- [PR Comment cli](#pr-comment-cli)
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

```
docker run -it --rm -e GITHUB_TOKEN="GITHUB_TOKEN" -v /tmp/docker:/tmp kart clone linz/topographic-data
```

**Clone specific branch or commit:**
```
docker run -it --rm -e GITHUB_TOKEN="GITHUB_TOKEN" -v /tmp/docker:/tmp kart clone linz/topographic-data --ref feature-branch
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

**Specific dataset(s):**
```
docker run -it --rm -v /tmp/docker:/tmp kart export marine airport
```

**Export from specific commit:**
```
docker run -it --rm -v /tmp/docker:/tmp kart export --ref commit-sha
```

## PR Comment cli

Add or update a pull request comment with diff results.

**Auto-detect PR and repo:**
```
docker run -it --rm -e GITHUB_TOKEN="GITHUB_TOKEN" -v /tmp/docker:/tmp kart pr-comment
```

**Specify PR number and repo:**
```
docker run -it --rm -e GITHUB_TOKEN="GITHUB_TOKEN" -v /tmp/docker:/tmp kart pr-comment --pr 123 --repo linz/topographic-data
```

**Custom comment body file:**
```
docker run -it --rm -e GITHUB_TOKEN="GITHUB_TOKEN" -v /tmp/docker:/tmp kart pr-comment custom_summary.md
```

## Version cli

Output the kart version.
```
docker run -it --rm -v /tmp/docker:/tmp kart version
```

## Sample GitHub Actions Workflow

Here's a complete example of how to use the kart CLI commands in a GitHub Actions workflow for pull request checks:

```yaml
name: pr-checks

on: pull_request

jobs:
  diff-qc-export:
    runs-on: ubuntu-latest
    container:
      image: ghcr.io/linz/topographic-system/kart

    steps:
      - name: Check kart version
        run: node /scripts/index.js version
        
      - name: Clone repository
        run: node /scripts/index.js clone ${{ github.event.repository.clone_url }} --ref ${{ github.head_ref }}
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          
      - name: Generate diff
        run: node /scripts/index.js diff
        
      - name: Comment on PR
        run: node /scripts/index.js pr-comment
        env:
          GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
          
      - name: Export datasets
        run: node /scripts/index.js export
```

This workflow will:
1. Check the kart version for debugging
2. Clone the repository with the PR branch
3. Generate a diff comparing the PR changes against master
4. Post a comment on the PR with the diff results
5. Export all datasets as geopackages

The workflow runs on every pull request and provides automated quality checks and diff visualization for topographic data changes.

