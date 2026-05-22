from unittest.mock import MagicMock, patch

from kart_import.defs.assets.bundle import fetch_bundle_head


def test_fetch_bundle_head_valid():
    # Simulate a standard git bundle header containing both HEAD and refs/heads/main.
    mock_data = (
        b"# v2 git bundle\n"
        b"-894835e99fbe5c7df3280d5f8425c09ba6c2a801 prerequisite message\n"
        b"894835e99fbe5c7df3280d5f8425c09ba6c2a801 HEAD\n"
        b"1234567890abcdef1234567890abcdef12345678 refs/heads/main\n"
        b"\n"
        b"PACK\x00\x00\x00..."
    )

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = mock_data
        mock_urlopen.return_value.__enter__.return_value = mock_response

        sha = fetch_bundle_head("nz_chatham_island_airport_polygons")
        assert sha == "894835e99fbe5c7df3280d5f8425c09ba6c2a801"


def test_fetch_bundle_head_fallback():
    # Simulate a bundle header without explicit HEAD, which now raises an Exception.
    mock_data = b"# v2 git bundle\n1234567890abcdef1234567890abcdef12345678 refs/heads/main\n\n"

    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = mock_data
        mock_urlopen.return_value.__enter__.return_value = mock_response

        import pytest

        with pytest.raises(Exception, match="No HEAD found in bundle refs"):
            fetch_bundle_head("nz_chatham_island_airport_polygons")


def test_fetch_bundle_head_invalid():
    # Simulate a header where parsing breaks due to binary/invalid format immediately.
    with patch("urllib.request.urlopen") as mock_urlopen:
        mock_response = MagicMock()
        mock_response.read.return_value = b"PACK\x00\x00\x00..."
        mock_urlopen.return_value.__enter__.return_value = mock_response

        import pytest

        with pytest.raises(Exception, match="No HEAD found in bundle refs"):
            fetch_bundle_head("nz_chatham_island_airport_polygons")
