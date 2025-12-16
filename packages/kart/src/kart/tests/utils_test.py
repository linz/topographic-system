import os
import tempfile
import shutil
from _pytest.monkeypatch import MonkeyPatch

from kart import utils


def test_repository_name_from_url() -> None:
    """
    Test that the repository name is correctly extracted from various URL formats.
    """
    assert utils.repository_name_from_url("https://github.com/org/repo.git") == "repo"
    assert utils.repository_name_from_url("https://github.com/org/repo") == "repo"
    assert utils.repository_name_from_url("git@github.com:org/repo.git") == "repo"
    assert utils.repository_name_from_url("git@github.com:org/repo") == "repo"


def test_repository_data_path() -> None:
    """
    Test that the repository data path is correctly constructed.
    """
    url = "https://github.com/org/repo.git"
    expected = "/tmp/data/repo"
    assert utils.repository_data_path(url) == expected


def test_time_in_ms() -> None:
    """
    Test that time_in_ms returns a float and that subsequent calls are non-decreasing.
    """
    t1 = utils.time_in_ms()
    t2 = utils.time_in_ms()
    assert t2 >= t1
    assert isinstance(t1, float)


def test_ensure_git_credentials(monkeypatch: MonkeyPatch) -> None:
    """
    Test that git credentials are correctly written when GITHUB_TOKEN is set.
    """
    temp_home = tempfile.mkdtemp()
    monkeypatch.setenv("HOME", temp_home)
    monkeypatch.setenv("GITHUB_TOKEN", "dummy_token")
    monkeypatch.setenv("GITHUB_USER", "dummy_user")
    utils.ensure_git_credentials()
    cred_path = os.path.join(temp_home, ".git-credentials")
    config_path = os.path.join(temp_home, ".gitconfig")
    with open(cred_path) as f:
        content = f.read()
        assert "dummy_user:dummy_token" in content
    with open(config_path) as f:
        content = f.read()
        assert "helper = store" in content
    shutil.rmtree(temp_home)
