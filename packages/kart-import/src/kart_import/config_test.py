import pytest
from pydantic import ValidationError

from .config import FieldSpec, Source, Theme, ThemeDataset, get_dataset_name, validate_theme_joins


def _theme_with_join(join_columns):
    return Theme.model_validate(
        {
            "name": "road_line",
            "target_repo": "topographic-data",
            "target_epsg": "EPSG:4167",
            "lookups": [
                {
                    "name": "road_lkp",
                    "source": {"url": "git@github.com:linz/topographic-source-data", "dataset": "linz_road_cl"},
                    "key": "t50_fid",
                    "columns": {"width_indicator": "$width", "name_id": "$name_id"},
                }
            ],
            "datasets": [
                {
                    "source": "kart@data.koordinates.com:linz/nz-road-centrelines-topo-150k",
                    "joins": [{"lookup": "road_lkp", "left_on": "t50_fid", "columns": join_columns}],
                }
            ],
        }
    )


def test_string_source_is_coerced_to_url():
    td = ThemeDataset.model_validate({"source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"})
    assert td.source.url == "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"
    assert td.source.dataset is None


def test_string_source_derives_name():
    td = ThemeDataset.model_validate({"source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"})
    assert td.name == "nz_airport_polygons"


def test_dict_source_keeps_explicit_dataset_and_name():
    td = ThemeDataset.model_validate(
        {
            "name": "lamps_linz_road_cl",
            "source": {"url": "git@github.com:linz/topographic-source-data", "dataset": "linz_road_cl"},
        }
    )
    assert td.source.url == "git@github.com:linz/topographic-source-data"
    assert td.source.dataset == "linz_road_cl"
    assert td.name == "lamps_linz_road_cl"


def test_non_koordinates_source_without_name_is_rejected():
    with pytest.raises(ValidationError):
        ThemeDataset.model_validate(
            {"source": {"url": "git@github.com:linz/topographic-source-data", "dataset": "linz_road_cl"}}
        )


def test_get_dataset_name_rejects_non_koordinates_source():
    with pytest.raises(ValueError, match="set 'name:' explicitly"):
        get_dataset_name(Source(url="git@github.com:linz/topographic-source-data"))


def test_lookup_and_join_parse():
    theme = Theme.model_validate(
        {
            "name": "road_line",
            "target_repo": "topographic-data",
            "target_epsg": "EPSG:4167",
            "lookups": [
                {
                    "name": "road_lkp",
                    "source": {"url": "git@github.com:linz/topographic-source-data", "dataset": "linz_road_cl"},
                    "key": "t50_fid",
                    "columns": {"width_indicator": "$width", "name_id": "$name_id"},
                }
            ],
            "datasets": [
                {
                    "source": "kart@data.koordinates.com:linz/nz-road-centrelines-topo-150k",
                    "joins": [{"lookup": "road_lkp", "left_on": "t50_fid"}],
                }
            ],
        }
    )
    assert theme.lookups[0].name == "road_lkp"
    assert theme.lookups[0].key == "t50_fid"
    assert theme.datasets[0].joins[0].lookup == "road_lkp"
    assert theme.datasets[0].joins[0].left_on == "t50_fid"
    assert theme.datasets[0].joins[0].columns is None  # default: all lookup columns


def test_validate_theme_joins_accepts_known_columns():
    validate_theme_joins(_theme_with_join(["width_indicator"]))  # subset of lookup columns -> ok
    validate_theme_joins(_theme_with_join(None))  # None -> all columns -> ok


def test_validate_theme_joins_rejects_unknown_column():
    with pytest.raises(ValueError, match="unknown columns \\['nope'\\]"):
        validate_theme_joins(_theme_with_join(["width_indicator", "nope"]))


def test_validate_theme_joins_rejects_unknown_lookup():
    theme = _theme_with_join(None)
    theme.datasets[0].joins[0].lookup = "missing_lkp"
    with pytest.raises(ValueError, match="unknown lookup 'missing_lkp'"):
        validate_theme_joins(theme)


def test_field_spec_parses_scalar_shorthand():
    assert FieldSpec.parse("$").source == "$"
    assert FieldSpec.parse("$hway_num").source == "$hway_num"
    assert FieldSpec.parse("road").source == "road"
    assert FieldSpec.parse(1).source == 1
    spec = FieldSpec.parse(None)
    assert spec.source is None and spec.default is None


def test_field_spec_parses_dict_with_default():
    spec = FieldSpec.parse({"source": "$", "default": "Unknown"})
    assert spec.source == "$"
    assert spec.default == "Unknown"


def test_field_spec_rejects_unknown_keys():
    with pytest.raises(ValidationError):
        FieldSpec.parse({"surce": "$"})  # typo'd key


def test_fixups_default_to_empty():
    td = ThemeDataset.model_validate({"source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"})
    assert td.fixups == []


def test_fixup_is_parsed():
    td = ThemeDataset.model_validate(
        {
            "source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k",
            "fixups": [{"fn": "change_type_to_none", "releases": [64, 65]}],
        }
    )
    assert td.fixups[0].fn == "change_type_to_none"
    assert td.fixups[0].releases == [64, 65]


def test_unknown_fixup_is_rejected_at_load():
    with pytest.raises(ValidationError):
        ThemeDataset.model_validate(
            {
                "source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k",
                "fixups": [{"fn": "no_such_fixup"}],
            }
        )


def test_theme_dataset_validates_mapping_at_load():
    with pytest.raises(ValidationError):
        ThemeDataset.model_validate(
            {
                "source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k",
                "mapping": {"name": {"surce": "$"}},  # bad spec surfaces at load
            }
        )


SOURCE = "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k"


def _with_corrections(corrections: list[dict]) -> ThemeDataset:
    return ThemeDataset.model_validate({"source": SOURCE, "corrections": corrections})


def test_corrections_default_to_empty():
    assert ThemeDataset.model_validate({"source": SOURCE}).corrections == []


def test_correction_replace_is_parsed():
    td = _with_corrections([{"column": "way_count", "replace": {"1": "one way"}}])
    assert td.corrections[0].column == "way_count"
    assert td.corrections[0].replace == {"1": "one way"}


def test_correction_set_where_is_parsed():
    td = _with_corrections([{"column": "support_type", "set": "pole", "where": {"type": "telephone"}}])
    correction = td.corrections[0]
    assert correction.set_value == "pole"
    assert correction.where == {"type": "telephone"}


def test_correction_with_both_replace_and_set_is_rejected():
    with pytest.raises(ValidationError, match="exactly one"):
        _with_corrections([{"column": "c", "replace": {"a": "b"}, "set": "x", "where": {"c": "a"}}])


def test_correction_with_neither_replace_nor_set_is_rejected():
    with pytest.raises(ValidationError, match="exactly one"):
        _with_corrections([{"column": "c"}])


def test_correction_set_without_where_is_rejected():
    with pytest.raises(ValidationError, match="requires 'where'"):
        _with_corrections([{"column": "c", "set": "x"}])


def test_correction_replace_with_where_is_rejected():
    with pytest.raises(ValidationError, match="must not use 'where'"):
        _with_corrections([{"column": "c", "replace": {"a": "b"}, "where": {"c": "a"}}])
