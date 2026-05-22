from dagster import asset, AssetExecutionContext, AssetKey, AssetsDefinition

import geopandas as gpd
import pandas as pd

from ..config import WORKING_THEME_DIR, WORKING_TRANSFORM_DIR, Release, Theme, get_releases, get_themes


def make_theme_asset(context: AssetExecutionContext, theme: Theme, releases: list[Release]):
    for release in releases:
        context.log.info(f"Merging theme: {theme.name} for release {release.id}")

        release_dir = WORKING_THEME_DIR / f"release_{release.id}"
        release_dir.mkdir(parents=True, exist_ok=True)
        output_geojson = release_dir / f"{theme.name}.geojson"
        gdfs = []

        if output_geojson.exists():
            output_geojson.unlink()

        for dataset in theme.datasets:
            geojson_path = WORKING_TRANSFORM_DIR / f"release_{release.id}" / f"{dataset.name}.json"
            gdf = gpd.read_file(geojson_path)
            if gdf.empty:
                context.log.info(f"{dataset.name} (release {release.id}) is empty. Skipping.")
                continue

            context.log.info(f"{dataset.name} (release {release.id}): {len(gdf)} features")
            gdfs.append(gdf)

        if not gdfs:
            context.log.warning(f"No data found for theme {theme.name} release {release.id}.")
            return None

        merged = gpd.GeoDataFrame(pd.concat(gdfs, ignore_index=True), crs=gdfs[0].crs)

        # Stable sorting to keep row order predictable
        if "id" in merged.columns:
            merged = merged.sort_values(by=["id"]).reset_index(drop=True)

        context.log.info(f"Writing {len(merged)} total features → {output_geojson}")

        # Explicitly remove fid if it exists and ensure index is not written
        if "fid" in merged.columns:
            merged = merged.drop(columns=["fid"])

        merged.to_file(output_geojson, driver="GeoJSON", index=False)


def make_theme_release_asset(theme: Theme, releases: list[Release]) -> AssetsDefinition:
    deps = []
    for dataset in theme.datasets:
        deps.append(AssetKey(f"transform_{dataset.name}"))

    @asset(name=f"theme_{theme.name}", group_name="themes", deps=deps)
    def _theme_asset(context: AssetExecutionContext):
        return make_theme_asset(context, theme, releases)

    return _theme_asset


theme_assets = []
releases = get_releases()
for t in get_themes():
    if t.name == "all":
        continue

    theme_assets.append(make_theme_release_asset(t, releases))
