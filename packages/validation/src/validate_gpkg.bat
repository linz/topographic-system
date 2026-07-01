REM topo_validation_qgis.bat "C:\Data\topoedit\topographic-data\topographic-data.gpkg" --v "C:\Data\topoedit\topology-validation" 174.81 -41.31 174.82 -41.30

set BBOX=

if not "%~3"=="" set BBOX=--bbox %3 %4 %5 %6

call %LOCALAPPDATA%\Programs\OSGeo4W\bin\o4w_env.bat

echo %BBOX%

%LOCALAPPDATA%\Programs\OSGeo4W\apps\Python312\python.exe validate_gpkg_cli.py --config-file ".\config\default_config.json" --db-path %1 --v --output-dir %2 %BBOX%

