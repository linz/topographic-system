# Changelog

## 1.0.0 (2026-04-22)


### Features

* **kart:** Update kart flow to use stac push. BM-1625 ([#120](https://github.com/linz/topographic-system/issues/120)) ([ba04f4f](https://github.com/linz/topographic-system/commit/ba04f4f7224cc014b222b73b754cd2c2d1413b1d))
* **map:** Resolve STAC inconsistencies across the codebase. BM-1578 ([#87](https://github.com/linz/topographic-system/issues/87)) ([9e09afc](https://github.com/linz/topographic-system/commit/9e09afc26ff5754bd17ef6688e897029e767d142))
* **map:** Stac push command to copy stac files and assets with storage stategy. BM-1619 ([#114](https://github.com/linz/topographic-system/issues/114)) ([9d1dc2f](https://github.com/linz/topographic-system/commit/9d1dc2f6ec198a6c821dd6243fd66abb6f105c92))
* **shared:** Add concurrency parameter for all the clis that have limit queue. BM-1599 ([#126](https://github.com/linz/topographic-system/issues/126)) ([0f7d32c](https://github.com/linz/topographic-system/commit/0f7d32c79445ee0870c7b131e4581733ce675736))


### Bug Fixes

* update `chunkd` to support multipart write to s3 from stream BM-1621 ([#115](https://github.com/linz/topographic-system/issues/115)) ([b89c9f7](https://github.com/linz/topographic-system/commit/b89c9f71dedaaa224387b4f9078978fda87c269e))


### Dependencies

* The following workspace dependencies were updated
  * dependencies
    * @linzjs/topographic-system-shared bumped from ^0.8.0 to ^0.9.0
