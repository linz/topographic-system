# Information about and guides to using WSLC to run docker containers on windows using native windows linux set up

DOCUMENTATION

Windows

Run schema validation locally.
Requires node.js installed
For this repo specifically, it declares a required Node version of about 24.x (engine is ^24.5.0), so use Node 24 to avoid runtime issues.
And install dependencies from repo root: > npm install

example checks
node -v = v24.15.0
npm -v = 11.12.1

Run location topographic-system
Run from command line

For windows - requires URL file style location if file not relative to command line location.

node packages/kart/src/index.ts validate-schema --schema schema/next/airport.json file:///c:/data/temp/airport.parquet

node packages/kart/src/index.ts validate-schema --schema file:///c:/Data/toposource/schema_model/airport.json file:///c:/data/temp/airport.parquet

Building docker locally using latest code

Dependencies:
wsl - should be installed by default Windows 11. Windows Subsystem for Linux.
wslc - this is the built in Linux container - info see: https://devblogs.microsoft.com/commandline/wsl-container-is-now-available-for-public-preview/

Running update will install the latest version

> wsl --version
> wsl --update

NOTE: while container in pre-release
You can now access the WSL container feature in our latest pre-release of WSL!
You can install this release right away by running

> wsl --update --pre-release

Create docker environment - BUILD IMAGE
run full docker build

npm run -w @linzjs/topographic-system-kart bundle (needed if code updated)
wslc build -t kart -f packages/kart/Dockerfile .

This full docker build a series of commands including the schema and topology validation
Running commands in windows

Assuming the build has included the schema juts need to point to the data.

Running SCHEMA VALIDATION

Against live schema

> wslc run --rm -it -v C:\data\temp:/data kart validate-schema --schema /schema/airport.json /data/airport.parquet

Against next schema

> wslc run --rm -it -v C:\data\temp:/data kart validate-schema --schema /schema/next/road_line.json /data/road_line.parquet

Target a schema folder

> wslc run --rm -it -v C:\data\temp\amcmenamin:/data -v C:\Data\toposource\schema_model:/schema kart validate-schema --schema /schema/airport.json /data/airport.parquet

HELP

> wslc run --rm -it kart --help

Other useful WSLC commands
LIST
wslc container ps -a
wslc image ls

DELETE
wslc container delete id/name
wslc image delete id/name

examples:
wslc image delete kart
wslc image delete 20649cd1bb45

Sessions
wslc system session list
wslc system session terminate

create a command line in container where kart is the image
wslc run --rm -it kart bash

Find a file - CHECK NEEDED
wslc run --rm kart find / -name "airport.json"

Running TOPOLOGY VALIDATION

Key is --entrypoint

Help

wslc run --rm -v c:/Data/toposource/topographic-data:/input -v c:/Data/toposource/validation-results:/output --entrypoint uv kart:latest run --directory /packages/validation python src/topographic_validation/cli.py --help


wslc run --rm -v c:/Data/toposource/topographic-data:/input -v c:/Data/toposource/validation-results:/output --entrypoint uv kart:latest run --directory /packages/validation python src/topographic_validation/cli.py --verbose --config-file config/default_config.json --bbox 174.824 -36.92 174.829 -36.919 --db-path /input/topographic-data.gpkg --output-dir /output

Clean Up if Fails - locked file


Optional verify after delete:

wslc run --rm -it -v c:/Data/toposource/topographic-data:/input topoval sh -lc "ls -la /input/topographic-data.gpkg\*"

Useful check commands:

Integrity check in container
wslc run --rm -it -v c:/Data/toposource/topographic-data:/input topoval uv run python -c "import sqlite3; c=sqlite3.connect('/input/topographic-data.gpkg'); print(c.execute('PRAGMA integrity_check').fetchone()[0])"
