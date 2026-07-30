import urllib.error
from unittest.mock import MagicMock, patch

import pytest

from .bundle import clone_from_bundle, download_bundle


def test_download_bundle_writes_content(tmp_path):
    """Response body is streamed to the target file."""
    target = tmp_path / "dataset.bundle"
    bundle_data = b"FAKE_BUNDLE_PACK_DATA"

    mock_response = MagicMock()
    mock_response.read.side_effect = [bundle_data, b""]  # data then EOF

    with (
        patch("urllib.request.urlopen") as mock_urlopen,
        patch("kart_import.git.bundle.env_bundle_url", return_value="https://cdn.example.com/dataset.bundle"),
    ):
        mock_urlopen.return_value.__enter__.return_value = mock_response
        download_bundle("dataset", target)

    assert target.exists()
    assert target.read_bytes() == bundle_data


def test_download_bundle_uses_env_url(tmp_path):
    """URL is sourced from env_bundle_url, called with the dataset name."""
    target = tmp_path / "dataset.bundle"

    mock_response = MagicMock()
    mock_response.read.return_value = b""

    with (
        patch("urllib.request.urlopen") as mock_urlopen,
        patch("kart_import.git.bundle.env_bundle_url", return_value="https://cdn.example.com/my_ds.bundle") as mock_env,
    ):
        mock_urlopen.return_value.__enter__.return_value = mock_response
        download_bundle("my_ds", target)

    mock_env.assert_called_once_with("my_ds")
    mock_urlopen.assert_called_once_with("https://cdn.example.com/my_ds.bundle")


def test_download_bundle_raises_on_http_error(tmp_path):
    """HTTP error responses (4xx/5xx) propagate as URLError."""
    target = tmp_path / "dataset.bundle"

    with (
        patch("urllib.request.urlopen") as mock_urlopen,
        patch("kart_import.git.bundle.env_bundle_url", return_value="https://cdn.example.com/dataset.bundle"),
    ):
        mock_urlopen.side_effect = urllib.error.HTTPError(url=None, code=404, msg="Not Found", hdrs=None, fp=None)
        with pytest.raises(urllib.error.URLError):
            download_bundle("dataset", target)


def test_clone_from_bundle_runs_git_clone(tmp_path):
    """Invokes git clone with the correct arguments and leaves a plain .git repo."""
    bundle_path = tmp_path / "repo.bundle"
    target_dir = tmp_path / "repo"

    with patch("kart_import.git.bundle.run_command") as mock_run:
        clone_from_bundle(bundle_path, target_dir)

    mock_run.assert_called_once_with(["git", "clone", str(bundle_path), str(target_dir), "--no-checkout"])
