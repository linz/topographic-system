REM topo_validation_qgis.bat --db-path "C:\Data\topoedit\topographic-data\topographic-data.gpkg" --v --output-dir "C:\Data\topoedit\topology-validation"

call %LOCALAPPDATA%\Programs\OSGeo4W\bin\o4w_env.bat

%LOCALAPPDATA%\Programs\OSGeo4W\apps\Python312\python.exe cli.py --config-file ".\config\default_config.json" %*

