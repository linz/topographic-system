# Changelog

## [0.9.0](https://github.com/linz/topographic-system/compare/topographic-system-shared-v0.8.0...topographic-system-shared-v0.9.0) (2026-04-22)


### Features

* **kart:** Update kart flow to use stac push. BM-1625 ([#120](https://github.com/linz/topographic-system/issues/120)) ([ba04f4f](https://github.com/linz/topographic-system/commit/ba04f4f7224cc014b222b73b754cd2c2d1413b1d))
* **map:** Resolve STAC inconsistencies across the codebase. BM-1578 ([#87](https://github.com/linz/topographic-system/issues/87)) ([9e09afc](https://github.com/linz/topographic-system/commit/9e09afc26ff5754bd17ef6688e897029e767d142))
* **shared:** Add concurrency parameter for all the clis that have limit queue. BM-1599 ([#126](https://github.com/linz/topographic-system/issues/126)) ([0f7d32c](https://github.com/linz/topographic-system/commit/0f7d32c79445ee0870c7b131e4581733ce675736))


### Bug Fixes

* add a temporal extent for datasets with no extent ([#124](https://github.com/linz/topographic-system/issues/124)) ([3fe7adc](https://github.com/linz/topographic-system/commit/3fe7adc0e8f41ee6a244576e22e9c63e4d7b751b))
* import directly from package entry point BM-1638 ([#123](https://github.com/linz/topographic-system/issues/123)) ([fdff98d](https://github.com/linz/topographic-system/commit/fdff98dd06d478bdfd464a4a8dddedcfea91a543))
* nztopo50 is the name of the product ([#112](https://github.com/linz/topographic-system/issues/112)) ([45e1135](https://github.com/linz/topographic-system/commit/45e113501bd994a29be7e9d91d0bc6becdc64234))
* update `chunkd` to support multipart write to s3 from stream BM-1621 ([#115](https://github.com/linz/topographic-system/issues/115)) ([b89c9f7](https://github.com/linz/topographic-system/commit/b89c9f71dedaaa224387b4f9078978fda87c269e))

## [0.8.0](https://github.com/linz/topographic-system/compare/topographic-system-shared-v0.7.0...topographic-system-shared-v0.8.0) (2026-03-22)


### Features

* add open telemetry ([#77](https://github.com/linz/topographic-system/issues/77)) ([b87004f](https://github.com/linz/topographic-system/commit/b87004fd6122526b01333f4497559e7ade272fba))
* instrument kart cli ([#79](https://github.com/linz/topographic-system/issues/79)) ([f132b3d](https://github.com/linz/topographic-system/commit/f132b3db0eafde1312d4a3e9b41b0f9f4e9a3231))
* kart flow implementation BM-1494 ([#86](https://github.com/linz/topographic-system/issues/86)) ([355ef1f](https://github.com/linz/topographic-system/commit/355ef1fbdd0be2ff7c73c9e33685c5cc6695b070))
* otel action ([#81](https://github.com/linz/topographic-system/issues/81)) ([bac740a](https://github.com/linz/topographic-system/commit/bac740a672952f611b08508e7e08f2778b9d323b))


### Bug Fixes

* isPullRequest logic BM-1580 ([#97](https://github.com/linz/topographic-system/issues/97)) ([a4dde75](https://github.com/linz/topographic-system/commit/a4dde75dede4b3f01ff7a7e135071ad21ce98409))
* STAC `latest` collection and assets BM-1507 ([#88](https://github.com/linz/topographic-system/issues/88)) ([cd02e56](https://github.com/linz/topographic-system/commit/cd02e5660dd94b1ed7ef626f14ba85a05acfeac6))

## [0.7.0](https://github.com/linz/topographic-system/compare/topographic-system-shared-v0.6.0...topographic-system-shared-v0.7.0) (2026-03-12)


### Features

* shared data release workflow for data repositories BM-1494 ([#59](https://github.com/linz/topographic-system/issues/59)) ([7c5704e](https://github.com/linz/topographic-system/commit/7c5704e868e96de6f346bb92ac3ba26d6205a5ce))

## [0.6.0](https://github.com/linz/topographic-system/compare/topographic-system-shared-v0.5.0...topographic-system-shared-v0.6.0) (2026-03-10)


### Features

* data prep stac generation BM-1499 ([#50](https://github.com/linz/topographic-system/issues/50)) ([5249ca6](https://github.com/linz/topographic-system/commit/5249ca64814db7456abd527c9e695c526593572f))


### Bug Fixes

* correct command usage ([#63](https://github.com/linz/topographic-system/issues/63)) ([ba63f34](https://github.com/linz/topographic-system/commit/ba63f34798185eeb0c94d929e1aaea2d27479357))
* downloads should run concurrently ([#60](https://github.com/linz/topographic-system/issues/60)) ([876d5fe](https://github.com/linz/topographic-system/commit/876d5fed7ee04bd645367d59f75a892dff8444f6))

## [0.5.0](https://github.com/linz/topographic-system/compare/topographic-system-shared-v0.4.0...topographic-system-shared-v0.5.0) (2026-03-02)


### Features

* Data validation wrapper BM-1519 ([#52](https://github.com/linz/topographic-system/issues/52)) ([7985e9c](https://github.com/linz/topographic-system/commit/7985e9ca0bb8bfb69f3929748694ccf8504445ce))
* **kart:** extract data from kart repo to gpkg BM-1447 ([#17](https://github.com/linz/topographic-system/issues/17)) ([0e13c2f](https://github.com/linz/topographic-system/commit/0e13c2f37f460ceee0eb2a6d6403e1e13abdfef4))
* **map:** cli for deploying qgis project into aws. BM-1394 ([#15](https://github.com/linz/topographic-system/issues/15)) ([9b5378e](https://github.com/linz/topographic-system/commit/9b5378ed8659dfbb86ffc8be64a05a13c2910abf))
* **map:** Create Stac files for the output files and simple tiff validation. BM-1392 ([#9](https://github.com/linz/topographic-system/issues/9)) ([88ebf31](https://github.com/linz/topographic-system/commit/88ebf313661b7ab0389daebbed3fad8b5f119392))
* **map:** Define the assets and push to s3 as Tar file. BM-1460 ([#35](https://github.com/linz/topographic-system/issues/35)) ([e897619](https://github.com/linz/topographic-system/commit/e897619f419bd32171207a2bd027fa90de091c41))
* **map:** Download data from qgis stac item and produce pdf. BM-1478 ([#41](https://github.com/linz/topographic-system/issues/41)) ([5c5e0e0](https://github.com/linz/topographic-system/commit/5c5e0e0546795feb0245ff62e86d3556ef03aafc))
* **map:** Download source vector data by the correct stac tag. BM-1497 ([#47](https://github.com/linz/topographic-system/issues/47)) ([b32a367](https://github.com/linz/topographic-system/commit/b32a3675a2c4b8dbdc6f00e5297602b31e6cb0bf))
* **map:** Failure with exit non zero process for github actions runs. ([#49](https://github.com/linz/topographic-system/issues/49)) ([f1d2fd2](https://github.com/linz/topographic-system/commit/f1d2fd231f40112f4148973b97888fc5fe9f1ed0))
* **map:** Refactoring map produce to create stac files and export pdf from stac ([#51](https://github.com/linz/topographic-system/issues/51)) ([699e55a](https://github.com/linz/topographic-system/commit/699e55a1dddc3fa2bf78def3ca7609dee124529d))
* **shared:** create STAC files from parquet assets BM-1477 ([#31](https://github.com/linz/topographic-system/issues/31)) ([a61f9bd](https://github.com/linz/topographic-system/commit/a61f9bde2941202ae8bf85ac8af440d055837e43))
* stac metadata from parquet BM-1511 ([#40](https://github.com/linz/topographic-system/issues/40)) ([8c6256b](https://github.com/linz/topographic-system/commit/8c6256bf938e0ad70ac4357a0e8ffd8be0b83836))
* Visual diff in PR BM-1424 ([#21](https://github.com/linz/topographic-system/issues/21)) ([9f0e004](https://github.com/linz/topographic-system/commit/9f0e00458954daa2f7bc06cf35420cd57f14a7a5))


### Bug Fixes

* correct more import names BM-1540 ([#55](https://github.com/linz/topographic-system/issues/55)) ([3cca581](https://github.com/linz/topographic-system/commit/3cca5815a3447245ba40d97cae7bea23d704d110))
* correct typescript monorepo structure ([#54](https://github.com/linz/topographic-system/issues/54)) ([9f5e3c0](https://github.com/linz/topographic-system/commit/9f5e3c0d88963794f4f504d07776c6bdba32c3f8))
* **shared:** mulitple package-lock json in shared causes diverged imports. ([#42](https://github.com/linz/topographic-system/issues/42)) ([71f27d7](https://github.com/linz/topographic-system/commit/71f27d7d66b9681b746a3458f59bfb0dd2e9387e))
* write updated STAC Item to s3 BM-1509 ([#39](https://github.com/linz/topographic-system/issues/39)) ([0207d5c](https://github.com/linz/topographic-system/commit/0207d5c52c65c72f3132ff5ff4cc61204631d9a1))

## [0.4.0](https://github.com/linz/topographic-system/compare/shared-v0.3.0...shared-v0.4.0) (2026-01-26)


### Features

* **shared:** create STAC files from parquet assets BM-1477 ([#31](https://github.com/linz/topographic-system/issues/31)) ([a61f9bd](https://github.com/linz/topographic-system/commit/a61f9bde2941202ae8bf85ac8af440d055837e43))

## [0.3.0](https://github.com/linz/topographic-system/compare/shared-v0.2.0...shared-v0.3.0) (2026-01-13)


### Features

* Visual diff in PR BM-1424 ([#21](https://github.com/linz/topographic-system/issues/21)) ([9f0e004](https://github.com/linz/topographic-system/commit/9f0e00458954daa2f7bc06cf35420cd57f14a7a5))

## [0.2.0](https://github.com/linz/topographic-system/compare/shared-v0.1.0...shared-v0.2.0) (2026-01-05)


### Features

* **kart:** extract data from kart repo to gpkg BM-1447 ([#17](https://github.com/linz/topographic-system/issues/17)) ([0e13c2f](https://github.com/linz/topographic-system/commit/0e13c2f37f460ceee0eb2a6d6403e1e13abdfef4))
* **map:** cli for deploying qgis project into aws. BM-1394 ([#15](https://github.com/linz/topographic-system/issues/15)) ([9b5378e](https://github.com/linz/topographic-system/commit/9b5378ed8659dfbb86ffc8be64a05a13c2910abf))
* **map:** Create Stac files for the output files and simple tiff validation. BM-1392 ([#9](https://github.com/linz/topographic-system/issues/9)) ([88ebf31](https://github.com/linz/topographic-system/commit/88ebf313661b7ab0389daebbed3fad8b5f119392))

## Changelog
