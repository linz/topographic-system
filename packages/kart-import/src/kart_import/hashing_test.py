from .hashing import read_hash_sidecar, sha256_file, write_hash_sidecar


def test_write_hash_sidecar_writes_the_files_sha256_next_to_it(tmp_path):
    path = tmp_path / "airport.fgb"
    path.write_bytes(b"some bytes")

    digest = write_hash_sidecar(path)

    assert digest == sha256_file(path)
    assert (tmp_path / "airport.fgb.hash").read_text() == digest


def test_read_hash_sidecar_returns_what_was_written(tmp_path):
    path = tmp_path / "airport.fgb"
    path.write_bytes(b"some bytes")
    write_hash_sidecar(path)

    assert read_hash_sidecar(path) == sha256_file(path)


def test_read_hash_sidecar_returns_none_when_there_is_no_sidecar(tmp_path):
    path = tmp_path / "airport.fgb"
    path.write_bytes(b"some bytes")

    assert read_hash_sidecar(path) is None


def test_sha256_file_changes_with_content(tmp_path):
    path = tmp_path / "airport.fgb"

    path.write_bytes(b"one")
    first = sha256_file(path)
    path.write_bytes(b"two")
    second = sha256_file(path)

    assert first != second
