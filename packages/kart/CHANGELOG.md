# Changelog

## [0.8.0](https://github.com/linz/topographic-system/compare/topographic-system-kart-v0.7.0...topographic-system-kart-v0.8.0) (2026-03-26)


### Features

* add logger statements to surface a command's runtime status BM-1584 ([#89](https://github.com/linz/topographic-system/issues/89)) ([94194a7](https://github.com/linz/topographic-system/commit/94194a7941da847c39b49e4be5b47637c99b7b4e))


### Bug Fixes

* kart to use geometry column name BM-1588 ([#100](https://github.com/linz/topographic-system/issues/100)) ([2eb545d](https://github.com/linz/topographic-system/commit/2eb545d4b54e37b71d46d1c5d1bbb3c54ef8887d))
* make the integration tests run the same ([#96](https://github.com/linz/topographic-system/issues/96)) ([ab7787d](https://github.com/linz/topographic-system/commit/ab7787d3de114f7e28c216ece8b97c1fb687647d))

## [0.7.0](https://github.com/linz/topographic-system/compare/topographic-system-kart-v0.6.0...topographic-system-kart-v0.7.0) (2026-03-22)


### Features

* instrument kart cli ([#79](https://github.com/linz/topographic-system/issues/79)) ([f132b3d](https://github.com/linz/topographic-system/commit/f132b3db0eafde1312d4a3e9b41b0f9f4e9a3231))
* kart flow implementation BM-1494 ([#86](https://github.com/linz/topographic-system/issues/86)) ([355ef1f](https://github.com/linz/topographic-system/commit/355ef1fbdd0be2ff7c73c9e33685c5cc6695b070))
* otel action ([#81](https://github.com/linz/topographic-system/issues/81)) ([bac740a](https://github.com/linz/topographic-system/commit/bac740a672952f611b08508e7e08f2778b9d323b))


### Bug Fixes

* STAC `latest` collection and assets BM-1507 ([#88](https://github.com/linz/topographic-system/issues/88)) ([cd02e56](https://github.com/linz/topographic-system/commit/cd02e5660dd94b1ed7ef626f14ba85a05acfeac6))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @linzjs/topographic-system-shared bumped from ^0.7.0 to ^0.8.0

## [0.6.0](https://github.com/linz/topographic-system/compare/topographic-system-kart-v0.5.0...topographic-system-kart-v0.6.0) (2026-03-12)


### Features

* shared data release workflow for data repositories BM-1494 ([#59](https://github.com/linz/topographic-system/issues/59)) ([7c5704e](https://github.com/linz/topographic-system/commit/7c5704e868e96de6f346bb92ac3ba26d6205a5ce))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @linzjs/topographic-system-shared bumped from ^0.6.0 to ^0.7.0

## [0.5.0](https://github.com/linz/topographic-system/compare/topographic-system-kart-v0.4.0...topographic-system-kart-v0.5.0) (2026-03-10)


### Features

* data prep stac generation BM-1499 ([#50](https://github.com/linz/topographic-system/issues/50)) ([5249ca6](https://github.com/linz/topographic-system/commit/5249ca64814db7456abd527c9e695c526593572f))
* use action-kart v0.4.0 ([#70](https://github.com/linz/topographic-system/issues/70)) ([c46dcd2](https://github.com/linz/topographic-system/commit/c46dcd2236a5278a0f09ea0474f185556a5fbd6f))


### Bug Fixes

* correct command usage ([#63](https://github.com/linz/topographic-system/issues/63)) ([ba63f34](https://github.com/linz/topographic-system/commit/ba63f34798185eeb0c94d929e1aaea2d27479357))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @linzjs/topographic-system-shared bumped from ^0.5.0 to ^0.6.0

## [0.4.0](https://github.com/linz/topographic-system/compare/topographic-system-kart-v0.3.1...topographic-system-kart-v0.4.0) (2026-03-02)


### Features

* Data validation wrapper BM-1519 ([#52](https://github.com/linz/topographic-system/issues/52)) ([7985e9c](https://github.com/linz/topographic-system/commit/7985e9ca0bb8bfb69f3929748694ccf8504445ce))
* **data-prep:** data preparation of contour with landcover BM-1498 ([#45](https://github.com/linz/topographic-system/issues/45)) ([43bc351](https://github.com/linz/topographic-system/commit/43bc35160139aadfafe12953f90a7f396e1f5d0d))
* **kart:** create geoparquet files from kart gpkg export BM-1480 ([#33](https://github.com/linz/topographic-system/issues/33)) ([0990be5](https://github.com/linz/topographic-system/commit/0990be570195c233ebfb04ba5f7a2f769c1eedf9))
* **kart:** extract data from kart repo to gpkg BM-1447 ([#17](https://github.com/linz/topographic-system/issues/17)) ([0e13c2f](https://github.com/linz/topographic-system/commit/0e13c2f37f460ceee0eb2a6d6403e1e13abdfef4))
* **kart:** only export changed datasets BM-1479 ([#28](https://github.com/linz/topographic-system/issues/28)) ([6617ea4](https://github.com/linz/topographic-system/commit/6617ea4e81956dc6d2fbfcf78a0bc42ae3055e1e))
* python package to validate topographic data BM-1518 ([#44](https://github.com/linz/topographic-system/issues/44)) ([908e2cb](https://github.com/linz/topographic-system/commit/908e2cb7647e651fa4f17f42f1c6b41b088d911d))
* stac metadata from parquet BM-1511 ([#40](https://github.com/linz/topographic-system/issues/40)) ([8c6256b](https://github.com/linz/topographic-system/commit/8c6256bf938e0ad70ac4357a0e8ffd8be0b83836))
* Visual diff in PR BM-1424 ([#21](https://github.com/linz/topographic-system/issues/21)) ([9f0e004](https://github.com/linz/topographic-system/commit/9f0e00458954daa2f7bc06cf35420cd57f14a7a5))


### Bug Fixes

* correct typescript monorepo structure ([#54](https://github.com/linz/topographic-system/issues/54)) ([9f5e3c0](https://github.com/linz/topographic-system/commit/9f5e3c0d88963794f4f504d07776c6bdba32c3f8))
* **kart:** do not use kart helper BM-1492 ([#36](https://github.com/linz/topographic-system/issues/36)) ([709e2ce](https://github.com/linz/topographic-system/commit/709e2ce546480ba2b62f5afad723c3f4d41ddee2))
* **shared:** mulitple package-lock json in shared causes diverged imports. ([#42](https://github.com/linz/topographic-system/issues/42)) ([71f27d7](https://github.com/linz/topographic-system/commit/71f27d7d66b9681b746a3458f59bfb0dd2e9387e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @linzjs/topographic-system-shared bumped from ^0.4.0 to ^0.5.0

## [0.3.1](https://github.com/linz/topographic-system/compare/kart-v0.3.0...kart-v0.3.1) (2026-01-28)


### Bug Fixes

* **kart:** do not use kart helper BM-1492 ([#36](https://github.com/linz/topographic-system/issues/36)) ([709e2ce](https://github.com/linz/topographic-system/commit/709e2ce546480ba2b62f5afad723c3f4d41ddee2))

## [0.3.0](https://github.com/linz/topographic-system/compare/kart-v0.2.0...kart-v0.3.0) (2026-01-26)


### Features

* **kart:** create geoparquet files from kart gpkg export BM-1480 ([#33](https://github.com/linz/topographic-system/issues/33)) ([0990be5](https://github.com/linz/topographic-system/commit/0990be570195c233ebfb04ba5f7a2f769c1eedf9))
* **kart:** only export changed datasets BM-1479 ([#28](https://github.com/linz/topographic-system/issues/28)) ([6617ea4](https://github.com/linz/topographic-system/commit/6617ea4e81956dc6d2fbfcf78a0bc42ae3055e1e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @topographic-system/shared bumped from file:../shared to 0.4.0

## [0.2.0](https://github.com/linz/topographic-system/compare/kart-v0.1.0...kart-v0.2.0) (2026-01-13)


### Features

* **kart:** extract data from kart repo to gpkg BM-1447 ([#17](https://github.com/linz/topographic-system/issues/17)) ([0e13c2f](https://github.com/linz/topographic-system/commit/0e13c2f37f460ceee0eb2a6d6403e1e13abdfef4))
* Visual diff in PR BM-1424 ([#21](https://github.com/linz/topographic-system/issues/21)) ([9f0e004](https://github.com/linz/topographic-system/commit/9f0e00458954daa2f7bc06cf35420cd57f14a7a5))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @topographic-system/shared bumped from file:../shared to 0.3.0
