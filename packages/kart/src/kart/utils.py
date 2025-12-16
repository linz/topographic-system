import time
import os


def repository_name_from_url(repository_url: str) -> str:
    """Extract the repository name from its URL.
    Args:
        repository_url: The URL of the repository

    Returns:
        The name of the repository
    """
    return repository_url.split("/")[-1].replace(".git", "")


def repository_data_path(repository_url: str) -> str:
    """Extract the default data path for the given repository URL.
    Args:
        repository_url: The URL of the repository

    Returns:
         The /tmp/data/<repo_name> path for the given repository URL.
    """
    return os.path.join("/tmp/data", repository_name_from_url(repository_url))


def time_in_ms() -> float:
    """Get the current time in milliseconds.
    Returns:
        The current time in ms
    """
    return time.time() * 1000


def ensure_git_credentials() -> None:
    """If GITHUB_TOKEN is set, write ~/.git-credentials and ~/.gitconfig for git to use the token."""
    token = os.environ.get("GITHUB_TOKEN")
    github_user = os.environ.get("GITHUB_USER", "x-access-token")
    if token:
        home = os.path.expanduser("~")
        cred_path = os.path.join(home, ".git-credentials")
        config_path = os.path.join(home, ".gitconfig")
        # Write credentials file
        with open(cred_path, "w") as f:
            f.write(f"https://{github_user}:{token}@github.com\n")
        # Write config file to use store helper
        with open(config_path, "w") as f:
            f.write("[credential]\n    helper = store\n")
