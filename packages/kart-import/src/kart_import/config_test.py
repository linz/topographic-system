import pytest
from pydantic import ValidationError

from .config import FieldSpec, Source, ThemeDataset, get_dataset_name


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


def test_theme_dataset_validates_mapping_at_load():
    with pytest.raises(ValidationError):
        ThemeDataset.model_validate(
            {
                "source": "kart@data.koordinates.com:linz/nz-airport-polygons-topo-150k",
                "mapping": {"name": {"surce": "$"}},  # bad spec surfaces at load
            }
        )
