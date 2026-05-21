"""End-to-end tests: run each fixture scenario through the validator factory.

Discovers every scenario under fixtures/{examples,counterexamples}/,
writes temporary parquet and gpkg from the geojson files, and
asserts that
 - examples pass validation
 - counterexamples fail validation
"""

import json
import subprocess
from pathlib import Path
from importlib.resources import as_file

import geopandas as gpd  # type: ignore
import pytest  # type: ignore

FIXTURES_DIR = Path(__file__).parent / "fixtures"
CONFIG_PATH = Path(__file__).parent.parent / "config" / "default_config.json"


def _write_fixtures(geojson_files: list[Path], dest: Path) -> dict[str, str]:
    """Returns {"gpkg": path, "parquet": path}."""
    dest.mkdir(parents=True, exist_ok=True)
    gpkg_path = dest / "data.gpkg"
    for file in geojson_files:
        gdf = gpd.GeoDataFrame.from_file(file)
        gdf.to_file(gpkg_path, driver="GPKG", layer=file.stem)
        gdf.to_parquet(dest / f"{file.stem}.parquet")
    return {
        "gpkg": str(gpkg_path),
        "parquet": str(
            dest / "files.parquet"
        ),  # files.parquet is expected by validation CLI
    }


def _build_config(full_config: dict, rule_name: str, table: str, scenario: str) -> dict:
    """Extract just the relevant rule entry into a minimal config."""
    matching = [
        entry
        for entry in full_config[rule_name]
        if entry.get("layername") == scenario and entry.get("table") == table
    ]
    if not matching:
        raise LookupError(f"No config entry for {rule_name}/{table}/{scenario}")
    return {rule_name: matching}


def _discover_examples() -> list:
    params = []
    with as_file(FIXTURES_DIR) as fixtures_path:
        for scenario_dir in [
            *fixtures_path.glob("examples/*/*/*/*/"),
            *fixtures_path.glob("counterexamples/*/*/*/*/"),
        ]:
            scenario_id = scenario_dir.parts[-5:]
            geojsons = sorted(scenario_dir.glob("*.geojson"))
            if geojsons:
                params.append(
                    pytest.param(
                        scenario_dir,
                        id="-".join(scenario_id),
                    )
                )
            else:
                raise ValueError(f"No .geojson files found in {scenario_dir}")
    return params


@pytest.fixture(scope="session")
def full_config() -> dict:
    """Load the validation config once per session."""
    if CONFIG_PATH.is_file():
        with CONFIG_PATH.open() as fh:
            data = json.load(fh)
        assert isinstance(data, dict)
        return data
    raise FileNotFoundError(f"Config file not found: {CONFIG_PATH}")


@pytest.mark.parametrize("scenario_dir", _discover_examples())
def test_validation_e2e(scenario_dir, full_config, tmp_path, subtests):
    role, table, rule_name, scenario, test_case = scenario_dir.parts[-5:]
    minimal_config = _build_config(full_config, rule_name, table, scenario)
    config_file = tmp_path / "minimal_config.json"
    config_file.write_text(json.dumps(minimal_config, indent=2))

    geojson_files = sorted(scenario_dir.glob("*.geojson"))
    db_paths = _write_fixtures(geojson_files, tmp_path / "data")

    for file_format, db_path in db_paths.items():
        message = f"{scenario}:{role[0].upper()}:{test_case} ({file_format}): {role}/{table}/{rule_name}/{scenario}"
        with subtests.test(msg=message):
            out_dir = tmp_path / f"out_{file_format}"

            result = subprocess.run(
                [
                    "uv",
                    "run",
                    "topographic_validation",
                    "--db-path",
                    db_path,
                    "--config-file",
                    str(config_file),
                    "--output-dir",
                    str(out_dir),
                    "--report-only",
                ],
                capture_output=True,
                text=True,
                timeout=60,
            )

            assert result.returncode == 0, (
                f"CLI failed (exit {result.returncode}):\n{result.stderr}"
            )

            summary_file = out_dir / "validation_summary_report.json"
            assert summary_file.exists(), f"No summary report in {out_dir}"
            summary = json.loads(summary_file.read_text())

            if role == "examples":
                assert summary.get(rule_name) is False, (
                    f"expected no errors for {message}, "
                    f"but summary[{rule_name}] = {summary.get(rule_name)}"
                )
            else:
                assert summary.get(rule_name) is True, (
                    f"expected errors for {message}, "
                    f"but summary[{rule_name}] = {summary.get(rule_name)}"
                )
