import geopandas as gpd

from . import export


def test_export_empty_releases_shares_one_file(tmp_path, monkeypatch):
    """The `export` rule declares an output per release, so a dataset whose repo starts partway
    through the series needs a file for the releases predating it. One shared file, symlinked like
    the commit exports, so the transform fingerprint normalises them once."""
    monkeypatch.setattr(export, "WORKING_EXPORTS_DIR", tmp_path)

    export.export_empty_releases("ds", [30, 31, 32])

    links = [tmp_path / f"release_{r}" / "ds.json" for r in (30, 31, 32)]
    assert all(link.is_symlink() for link in links)
    assert len({link.resolve() for link in links}) == 1  # one file behind all three


def test_export_empty_releases_is_readable_as_an_empty_frame(tmp_path, monkeypatch):
    """What transform reads back, and what makes it emit an empty transform rather than pushing
    a featureless frame through the column-wise normalisers."""
    monkeypatch.setattr(export, "WORKING_EXPORTS_DIR", tmp_path)

    export.export_empty_releases("ds", [30])

    gdf = gpd.read_file(tmp_path / "release_30" / "ds.json", engine="pyogrio", use_arrow=True)
    assert gdf.empty


def test_export_empty_releases_with_no_releases_writes_nothing(tmp_path, monkeypatch):
    """The normal case: a dataset covering every release must not gain a stray empty export."""
    monkeypatch.setattr(export, "WORKING_EXPORTS_DIR", tmp_path)

    export.export_empty_releases("ds", [])

    assert list(tmp_path.iterdir()) == []


def test_link_release_export_replaces_a_dangling_link(tmp_path, monkeypatch):
    """Snakemake deletes the outputs of a failed job, so a rerun can find a link whose target is
    gone. Such a link reads as non-existent, and `symlink` onto it would fail with EEXIST."""
    monkeypatch.setattr(export, "WORKING_EXPORTS_DIR", tmp_path)
    target = tmp_path / "ds" / "commit.json"
    target.parent.mkdir()
    target.write_text("{}")

    export.link_release_export(target, "ds", 30)
    target.unlink()  # dangling
    target.write_text("{}")
    export.link_release_export(target, "ds", 30)

    assert (tmp_path / "release_30" / "ds.json").resolve() == target
