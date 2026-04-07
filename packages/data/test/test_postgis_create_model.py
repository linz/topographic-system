from pathlib import Path
import runpy
import sys
from types import SimpleNamespace

import pandas as pd


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "nztopo50"
    / "import"
    / "core"
    / "postgis_create_model.py"
)


def _fake_dataframe():
    return pd.DataFrame(
        [
            ["Transport_Layers", "Road_Line", "x", "SHAPE", "Polyline", 0],
            ["Transport_Layers", "Road_Line", "x", "feature_type", "STRING", 50],
            ["Transport_Layers", "Road_Line", "x", "lane_count", "INTEGER", 0],
            ["Transport_Layers", "Road_Line", "x", "t50_fid", "BIGINTEGER", 0],
            ["Transport_Layers", "Road_Line", "x", "is_active", "BOOLEAN", 0],
            ["Transport_Layers", "Road_Line", "x", "objectid", "INTEGER", 0],
        ],
        columns=["dataset", "layer", "name", "mapped_name", "fieldtype", "length"],
    )


def _run_script_and_capture_sql(monkeypatch, dataframe):
    executed_sql = []

    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql):
            executed_sql.append(sql)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def cursor(self):
            return FakeCursor()

        def commit(self):
            return None

    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda **_kwargs: FakeConnection()),
    )
    monkeypatch.setattr(pd, "read_excel", lambda _path: dataframe)

    runpy.run_path(str(SCRIPT_PATH), run_name="__main__")
    return executed_sql


def test_excel_to_layered_dict_groups_by_dataset_and_layer(monkeypatch):
    dataframe = _fake_dataframe()
    monkeypatch.setitem(
        sys.modules,
        "psycopg",
        SimpleNamespace(connect=lambda **_kwargs: None),
    )
    monkeypatch.setattr(pd, "read_excel", lambda _path: dataframe)

    module_globals = runpy.run_path(str(SCRIPT_PATH))
    excel_to_layered_dict = module_globals["excel_to_layered_dict"]
    result = excel_to_layered_dict("ignored.xlsx")

    assert "Transport" in result
    assert "Road_Line" in result["Transport"]
    assert result["Transport"]["Road_Line"]["feature_type"] == {
        "type": "STRING",
        "length": 50,
    }


def test_main_generates_drop_and_create_sql(monkeypatch):
    executed_sql = _run_script_and_capture_sql(monkeypatch, _fake_dataframe())

    assert any(
        sql == "DROP TABLE IF EXISTS release64.road_line CASCADE;" for sql in executed_sql
    )

    create_sql = next(
        sql
        for sql in executed_sql
        if sql.startswith("CREATE TABLE IF NOT EXISTS release64.road_line")
    )

    assert "topo_id uuid DEFAULT gen_random_uuid()" in create_sql
    assert "feature_type VARCHAR(50)" in create_sql
    assert "lane_count INTEGER" in create_sql
    assert "t50_fid BIGINT" in create_sql
    assert "is_active BOOLEAN" in create_sql
    assert "geometry geometry(LINESTRING, 2193)" in create_sql


def test_main_skips_objectid_field_in_create_sql(monkeypatch):
    executed_sql = _run_script_and_capture_sql(monkeypatch, _fake_dataframe())
    create_sql = next(
        sql
        for sql in executed_sql
        if sql.startswith("CREATE TABLE IF NOT EXISTS release64.road_line")
    )

    assert "objectid" not in create_sql.lower()
