## Prerequisite

The process use POSTGRES/POSTGIS database to host data. This makes it easy to check.

Kart installation is required.

Access to GitHub repos is required.

It requires local sources of data. The default location is under c:\data\topo50.

See the README_SOURCES.md file for further information.

The code is based on Python using the dependencies listed in `pyproject.toml`.

### Required Software:

- **PostgreSQL/PostGIS** - Spatial database for hosting topographic data
- **Python 3.8+** - Core scripting environment
- **GDAL/OGR** - Geospatial data abstraction library

### Python Dependencies:

See `pyproject.toml` for complete list of required packages.

### Local Conda Setup

This project uses conda-forge for the geospatial stack so that GDAL, pyogrio,
and geopandas stay binary-compatible on Windows.

Create or update the local environment with:

```powershell
C:\Users\AMcMenamin\AppData\Local\miniconda3\Scripts\conda.exe env create -f environment.yml
```

If the environment already exists, update it instead:

```powershell
C:\Users\AMcMenamin\AppData\Local\miniconda3\Scripts\conda.exe env update -f environment.yml --prune
```

After activation, select the `topo50` interpreter in VS Code. Avoid mixing this
environment with a separate `uv` or `.venv` install, because that can reintroduce
the GDAL DLL conflict.
