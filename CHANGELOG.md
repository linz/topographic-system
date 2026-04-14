# Changelog

## [0.6.0](https://github.com/linz/topographic-system/compare/v0.5.1...v0.6.0) (2026-03-02)


### Features

* Data validation wrapper BM-1519 ([#52](https://github.com/linz/topographic-system/issues/52)) ([7985e9c](https://github.com/linz/topographic-system/commit/7985e9ca0bb8bfb69f3929748694ccf8504445ce))
* **data-prep:** data preparation of contour with landcover BM-1498 ([#45](https://github.com/linz/topographic-system/issues/45)) ([43bc351](https://github.com/linz/topographic-system/commit/43bc35160139aadfafe12953f90a7f396e1f5d0d))
* **map:** Define the assets and push to s3 as Tar file. BM-1460 ([#35](https://github.com/linz/topographic-system/issues/35)) ([e897619](https://github.com/linz/topographic-system/commit/e897619f419bd32171207a2bd027fa90de091c41))
* **map:** Download data from qgis stac item and produce pdf. BM-1478 ([#41](https://github.com/linz/topographic-system/issues/41)) ([5c5e0e0](https://github.com/linz/topographic-system/commit/5c5e0e0546795feb0245ff62e86d3556ef03aafc))
* **map:** Download source vector data by the correct stac tag. BM-1497 ([#47](https://github.com/linz/topographic-system/issues/47)) ([b32a367](https://github.com/linz/topographic-system/commit/b32a3675a2c4b8dbdc6f00e5297602b31e6cb0bf))
* **map:** Failure with exit non zero process for github actions runs. ([#49](https://github.com/linz/topographic-system/issues/49)) ([f1d2fd2](https://github.com/linz/topographic-system/commit/f1d2fd231f40112f4148973b97888fc5fe9f1ed0))
* **map:** Refactoring map produce to create stac files and export pdf from stac ([#51](https://github.com/linz/topographic-system/issues/51)) ([699e55a](https://github.com/linz/topographic-system/commit/699e55a1dddc3fa2bf78def3ca7609dee124529d))
* python package to validate topographic data BM-1518 ([#44](https://github.com/linz/topographic-system/issues/44)) ([908e2cb](https://github.com/linz/topographic-system/commit/908e2cb7647e651fa4f17f42f1c6b41b088d911d))
* stac metadata from parquet BM-1511 ([#40](https://github.com/linz/topographic-system/issues/40)) ([8c6256b](https://github.com/linz/topographic-system/commit/8c6256bf938e0ad70ac4357a0e8ffd8be0b83836))


### Bug Fixes

* correct more import names BM-1540 ([#55](https://github.com/linz/topographic-system/issues/55)) ([3cca581](https://github.com/linz/topographic-system/commit/3cca5815a3447245ba40d97cae7bea23d704d110))
* correct typescript monorepo structure ([#54](https://github.com/linz/topographic-system/issues/54)) ([9f5e3c0](https://github.com/linz/topographic-system/commit/9f5e3c0d88963794f4f504d07776c6bdba32c3f8))
* produce output location BM-1457 ([#32](https://github.com/linz/topographic-system/issues/32)) ([4ce25f9](https://github.com/linz/topographic-system/commit/4ce25f9a7a83d262826a9bf0efd90deefd7e0ff1))
* remove data prep package file BM-1540 ([#56](https://github.com/linz/topographic-system/issues/56)) ([ee38839](https://github.com/linz/topographic-system/commit/ee38839e6740a30f3fcfa9d0fd36a36d6d7b6db0))
* **shared:** mulitple package-lock json in shared causes diverged imports. ([#42](https://github.com/linz/topographic-system/issues/42)) ([71f27d7](https://github.com/linz/topographic-system/commit/71f27d7d66b9681b746a3458f59bfb0dd2e9387e))
* **validation:** update validation package name in release configuration ([#48](https://github.com/linz/topographic-system/issues/48)) ([dade7e2](https://github.com/linz/topographic-system/commit/dade7e2c373ecc2a59a62a23e0ceb8b7c3eabc51))
* write updated STAC Item to s3 BM-1509 ([#39](https://github.com/linz/topographic-system/issues/39)) ([0207d5c](https://github.com/linz/topographic-system/commit/0207d5c52c65c72f3132ff5ff4cc61204631d9a1))

## [0.5.1](https://github.com/linz/topographic-system/compare/v0.5.0...v0.5.1) (2026-01-28)


### Bug Fixes

* **kart:** do not use kart helper BM-1492 ([#36](https://github.com/linz/topographic-system/issues/36)) ([709e2ce](https://github.com/linz/topographic-system/commit/709e2ce546480ba2b62f5afad723c3f4d41ddee2))

## [0.5.0](https://github.com/linz/topographic-system/compare/v0.4.0...v0.5.0) (2026-01-26)


### Features

* **kart:** create geoparquet files from kart gpkg export BM-1480 ([#33](https://github.com/linz/topographic-system/issues/33)) ([0990be5](https://github.com/linz/topographic-system/commit/0990be570195c233ebfb04ba5f7a2f769c1eedf9))
* **kart:** only export changed datasets BM-1479 ([#28](https://github.com/linz/topographic-system/issues/28)) ([6617ea4](https://github.com/linz/topographic-system/commit/6617ea4e81956dc6d2fbfcf78a0bc42ae3055e1e))
* **map:** Add derived from githash stac link for the latest stac file. BM-1472 ([#27](https://github.com/linz/topographic-system/issues/27)) ([f3fac84](https://github.com/linz/topographic-system/commit/f3fac84504141ebda61e19a2eff3557781bc6787))
* **shared:** create STAC files from parquet assets BM-1477 ([#31](https://github.com/linz/topographic-system/issues/31)) ([a61f9bd](https://github.com/linz/topographic-system/commit/a61f9bde2941202ae8bf85ac8af440d055837e43))

## [0.4.0](https://github.com/linz/topographic-system/compare/v0.3.0...v0.4.0) (2026-01-13)


### Features

* **map:** Generate png files from the qgis project for screenshots. BM-1349 ([#20](https://github.com/linz/topographic-system/issues/20)) ([2e9b458](https://github.com/linz/topographic-system/commit/2e9b458a6b767fe8e409c0e82f97b9a452b6d595))
* Visual diff in PR BM-1424 ([#21](https://github.com/linz/topographic-system/issues/21)) ([9f0e004](https://github.com/linz/topographic-system/commit/9f0e00458954daa2f7bc06cf35420cd57f14a7a5))


### Bug Fixes

* init arch keyring BM-1474 ([#26](https://github.com/linz/topographic-system/issues/26)) ([b42dfef](https://github.com/linz/topographic-system/commit/b42dfefd378691fc1c04857e43360ea0dca670f5))

## [0.3.0](https://github.com/linz/topographic-system/compare/v0.2.0...v0.3.0) (2026-01-05)


### Features

* **kart:** extract data from kart repo to gpkg BM-1447 ([#17](https://github.com/linz/topographic-system/issues/17)) ([0e13c2f](https://github.com/linz/topographic-system/commit/0e13c2f37f460ceee0eb2a6d6403e1e13abdfef4))
* **map:** Add Download cli and list mapsheet clis for argo workflow. BM-1393 ([#11](https://github.com/linz/topographic-system/issues/11)) ([1eb12a6](https://github.com/linz/topographic-system/commit/1eb12a6603ed838ff321d6763985475a36902bcc))
* **map:** cli for deploying qgis project into aws. BM-1394 ([#15](https://github.com/linz/topographic-system/issues/15)) ([9b5378e](https://github.com/linz/topographic-system/commit/9b5378ed8659dfbb86ffc8be64a05a13c2910abf))
* **map:** Create Stac files for the output files and simple tiff validation. BM-1392 ([#9](https://github.com/linz/topographic-system/issues/9)) ([88ebf31](https://github.com/linz/topographic-system/commit/88ebf313661b7ab0389daebbed3fad8b5f119392))
* python configuration and CI BM-1456 ([#12](https://github.com/linz/topographic-system/issues/12)) ([33fab64](https://github.com/linz/topographic-system/commit/33fab642ac5e2565847a033c6e174f85b34c8167))


### Bug Fixes

* set packages and id-token write permission BM-1464 ([#19](https://github.com/linz/topographic-system/issues/19)) ([9add832](https://github.com/linz/topographic-system/commit/9add83248234a02fe6657e4bb5e43febb4204a2b))

## [0.2.0](https://github.com/linz/topographic-system/compare/v0.1.0...v0.2.0) (2025-12-01)


### Features

* Enable the map production cli in argo. BM-1392 ([#3](https://github.com/linz/topographic-system/issues/3)) ([9401cca](https://github.com/linz/topographic-system/commit/9401ccabf16c74c03bdb478f67eb68d4b3254685))


### Bug Fixes

* release-please remove component from root ([#6](https://github.com/linz/topographic-system/issues/6)) ([56d1055](https://github.com/linz/topographic-system/commit/56d10558e43a49cb245d7f0cb74a97ba19fec28b))

## [0.1.0](https://github.com/linz/topographic-system/compare/core-v0.0.1...core-v0.1.0) (2025-11-30)


### Features

* initial qgis map export BM-1410 ([#1](https://github.com/linz/topographic-system/issues/1)) ([7b719a2](https://github.com/linz/topographic-system/commit/7b719a2c56a55422a9c5c43050eab48b47ae3e0e))
* map productions docker container publishing and release BM-1391 ([#2](https://github.com/linz/topographic-system/issues/2)) ([ee68642](https://github.com/linz/topographic-system/commit/ee686421320f981d40d7a60d6d794bcbf2d95532))


### Bug Fixes

* github containers workflow with no tags created ([#4](https://github.com/linz/topographic-system/issues/4)) ([c4724e9](https://github.com/linz/topographic-system/commit/c4724e9c798a2fd0a85412dc9308432c5419f52e))
