@echo off
REM Example batch file to run the GeoParquet changelog CLI.
REM Update these values for your environment before running.

set CURRENT_RELEASE_NAME=release64
set PREVIOUS_RELEASE_NAME=release62
set RELEASE_DATE=2025-09-25
REM set CURRENT_RELEASE_PATH=C:/Data/temp/2025-09-25/release64
REM set PREVIOUS_RELEASE_PATH=C:/Data/temp/2025-02-05/release62
set CURRENT_RELEASE_PATH=https://d1jzh93b1t1cv.cloudfront.net/data/building/pull_request/pr-22/building.parquet
set PREVIOUS_RELEASE_PATH=https://d1jzh93b1t1cv.cloudfront.net/data/building/latest/building.parquet
set CHANGE_LOGS_PATH=c:/temp/data-changes

python generate_datachange_cli.py ^
  --current-release-name %CURRENT_RELEASE_NAME% ^
  --previous-release-name %PREVIOUS_RELEASE_NAME% ^
  --release-date %RELEASE_DATE% ^
  --current-release-path %CURRENT_RELEASE_PATH% ^
  --previous-release-path %PREVIOUS_RELEASE_PATH% ^
  --change-logs-path %CHANGE_LOGS_PATH%

REM Optional: add --use-hive-partitioning to enable hive output.
