# Changelog

## [0.6.0](https://github.com/linz/topographic-system/compare/topographic-system-map-v0.5.0...topographic-system-map-v0.6.0) (2026-03-02)


### Features

* Data validation wrapper BM-1519 ([#52](https://github.com/linz/topographic-system/issues/52)) ([7985e9c](https://github.com/linz/topographic-system/commit/7985e9ca0bb8bfb69f3929748694ccf8504445ce))
* Enable the map production cli in argo. BM-1392 ([#3](https://github.com/linz/topographic-system/issues/3)) ([9401cca](https://github.com/linz/topographic-system/commit/9401ccabf16c74c03bdb478f67eb68d4b3254685))
* initial qgis map export BM-1410 ([#1](https://github.com/linz/topographic-system/issues/1)) ([7b719a2](https://github.com/linz/topographic-system/commit/7b719a2c56a55422a9c5c43050eab48b47ae3e0e))
* **map:** Add derived from githash stac link for the latest stac file. BM-1472 ([#27](https://github.com/linz/topographic-system/issues/27)) ([f3fac84](https://github.com/linz/topographic-system/commit/f3fac84504141ebda61e19a2eff3557781bc6787))
* **map:** Add Download cli and list mapsheet clis for argo workflow. BM-1393 ([#11](https://github.com/linz/topographic-system/issues/11)) ([1eb12a6](https://github.com/linz/topographic-system/commit/1eb12a6603ed838ff321d6763985475a36902bcc))
* **map:** cli for deploying qgis project into aws. BM-1394 ([#15](https://github.com/linz/topographic-system/issues/15)) ([9b5378e](https://github.com/linz/topographic-system/commit/9b5378ed8659dfbb86ffc8be64a05a13c2910abf))
* **map:** Create Stac files for the output files and simple tiff validation. BM-1392 ([#9](https://github.com/linz/topographic-system/issues/9)) ([88ebf31](https://github.com/linz/topographic-system/commit/88ebf313661b7ab0389daebbed3fad8b5f119392))
* **map:** Define the assets and push to s3 as Tar file. BM-1460 ([#35](https://github.com/linz/topographic-system/issues/35)) ([e897619](https://github.com/linz/topographic-system/commit/e897619f419bd32171207a2bd027fa90de091c41))
* **map:** Download data from qgis stac item and produce pdf. BM-1478 ([#41](https://github.com/linz/topographic-system/issues/41)) ([5c5e0e0](https://github.com/linz/topographic-system/commit/5c5e0e0546795feb0245ff62e86d3556ef03aafc))
* **map:** Download source vector data by the correct stac tag. BM-1497 ([#47](https://github.com/linz/topographic-system/issues/47)) ([b32a367](https://github.com/linz/topographic-system/commit/b32a3675a2c4b8dbdc6f00e5297602b31e6cb0bf))
* **map:** Failure with exit non zero process for github actions runs. ([#49](https://github.com/linz/topographic-system/issues/49)) ([f1d2fd2](https://github.com/linz/topographic-system/commit/f1d2fd231f40112f4148973b97888fc5fe9f1ed0))
* **map:** Generate png files from the qgis project for screenshots. BM-1349 ([#20](https://github.com/linz/topographic-system/issues/20)) ([2e9b458](https://github.com/linz/topographic-system/commit/2e9b458a6b767fe8e409c0e82f97b9a452b6d595))
* **map:** Refactoring map produce to create stac files and export pdf from stac ([#51](https://github.com/linz/topographic-system/issues/51)) ([699e55a](https://github.com/linz/topographic-system/commit/699e55a1dddc3fa2bf78def3ca7609dee124529d))
* stac metadata from parquet BM-1511 ([#40](https://github.com/linz/topographic-system/issues/40)) ([8c6256b](https://github.com/linz/topographic-system/commit/8c6256bf938e0ad70ac4357a0e8ffd8be0b83836))


### Bug Fixes

* correct more import names BM-1540 ([#55](https://github.com/linz/topographic-system/issues/55)) ([3cca581](https://github.com/linz/topographic-system/commit/3cca5815a3447245ba40d97cae7bea23d704d110))
* correct typescript monorepo structure ([#54](https://github.com/linz/topographic-system/issues/54)) ([9f5e3c0](https://github.com/linz/topographic-system/commit/9f5e3c0d88963794f4f504d07776c6bdba32c3f8))
* init arch keyring BM-1474 ([#26](https://github.com/linz/topographic-system/issues/26)) ([b42dfef](https://github.com/linz/topographic-system/commit/b42dfefd378691fc1c04857e43360ea0dca670f5))
* produce output location BM-1457 ([#32](https://github.com/linz/topographic-system/issues/32)) ([4ce25f9](https://github.com/linz/topographic-system/commit/4ce25f9a7a83d262826a9bf0efd90deefd7e0ff1))
* **shared:** mulitple package-lock json in shared causes diverged imports. ([#42](https://github.com/linz/topographic-system/issues/42)) ([71f27d7](https://github.com/linz/topographic-system/commit/71f27d7d66b9681b746a3458f59bfb0dd2e9387e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @linzjs/topographic-system-shared bumped from ^0.4.0 to ^0.5.0

## [0.5.0](https://github.com/linz/topographic-system/compare/map-v0.4.0...map-v0.5.0) (2026-01-26)


### Features

* **map:** Add derived from githash stac link for the latest stac file. BM-1472 ([#27](https://github.com/linz/topographic-system/issues/27)) ([f3fac84](https://github.com/linz/topographic-system/commit/f3fac84504141ebda61e19a2eff3557781bc6787))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @topographic-system/shared bumped from 0.3.0 to 0.4.0

## [0.4.0](https://github.com/linz/topographic-system/compare/map-v0.3.0...map-v0.4.0) (2026-01-13)


### Features

* **map:** Generate png files from the qgis project for screenshots. BM-1349 ([#20](https://github.com/linz/topographic-system/issues/20)) ([2e9b458](https://github.com/linz/topographic-system/commit/2e9b458a6b767fe8e409c0e82f97b9a452b6d595))


### Bug Fixes

* init arch keyring BM-1474 ([#26](https://github.com/linz/topographic-system/issues/26)) ([b42dfef](https://github.com/linz/topographic-system/commit/b42dfefd378691fc1c04857e43360ea0dca670f5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @topographic-system/shared bumped from 0.2.0 to 0.3.0

## [0.3.0](https://github.com/linz/topographic-system/compare/map-v0.2.0...map-v0.3.0) (2026-01-05)


### Features

* **map:** Add Download cli and list mapsheet clis for argo workflow. BM-1393 ([#11](https://github.com/linz/topographic-system/issues/11)) ([1eb12a6](https://github.com/linz/topographic-system/commit/1eb12a6603ed838ff321d6763985475a36902bcc))
* **map:** cli for deploying qgis project into aws. BM-1394 ([#15](https://github.com/linz/topographic-system/issues/15)) ([9b5378e](https://github.com/linz/topographic-system/commit/9b5378ed8659dfbb86ffc8be64a05a13c2910abf))
* **map:** Create Stac files for the output files and simple tiff validation. BM-1392 ([#9](https://github.com/linz/topographic-system/issues/9)) ([88ebf31](https://github.com/linz/topographic-system/commit/88ebf313661b7ab0389daebbed3fad8b5f119392))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @topographic-system/shared bumped from 0.1.0 to 0.2.0

## [0.2.0](https://github.com/linz/topographic-system/compare/map-v0.1.0...map-v0.2.0) (2025-12-01)


### Features

* Enable the map production cli in argo. BM-1392 ([#3](https://github.com/linz/topographic-system/issues/3)) ([9401cca](https://github.com/linz/topographic-system/commit/9401ccabf16c74c03bdb478f67eb68d4b3254685))

## [0.1.0](https://github.com/linz/topographic-system/compare/map-v0.0.1...map-v0.1.0) (2025-11-30)


### Features

* initial qgis map export BM-1410 ([#1](https://github.com/linz/topographic-system/issues/1)) ([7b719a2](https://github.com/linz/topographic-system/commit/7b719a2c56a55422a9c5c43050eab48b47ae3e0e))
