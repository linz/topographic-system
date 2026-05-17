"""
ObstoreManager: A reusable class for managing object store operations.

Supports: LocalStore, S3Store (public and private).

Usage:
    manager = ObstoreManager()  # for local/cdn only
    manager = ObstoreManager(
        s3_bucket=..., s3_region=..., s3_access_key=..., s3_secret_key=..., s3_session_token=...
    )
    # Or pass S3 config directly to S3 methods
"""

from obstore.store import LocalStore, S3Store
from obstore import list as obstore_list
import os

class ObstoreManager:
    """Manages obstore operations for various storage backends."""

    def __init__(
        self,
        bucket=None,
        region=None,
        access_key=None,
        secret_key=None,
        session_token=None,
    ):
        self.bucket = bucket
        self.region = region
        self.access_key = access_key
        self.secret_key = secret_key
        self.session_token = session_token

    def _collect_files_and_items(self, store, prefix: str) -> tuple[list, list]:
        """Collect file paths and unique top-level folders from a store listing."""
        items = []
        files = []
        seen = set()
        for batch in obstore_list(store, prefix=prefix, chunk_size=100):
            for obj in batch:
                file_path = obj["path"]
                files.append(file_path)
                folder = file_path.split("/")[0]
                if folder not in seen:
                    seen.add(folder)
                    items.append(folder)
        return files, items

    def list_local_filesystem(self, prefix: str = "") -> tuple[list, list]:
        """
        List all items in a directory using obstore LocalStore.
        Use the bucket attribute as the base path.

        Args:
            prefix: The sub directory prefix to list

        Returns:
            A tuple containing a list of files and a list of unique items in the source directory
        """
        if not os.path.exists(self.bucket):
            print(f"Path does not exist: {self.bucket}")
            return [], []

        if not os.path.isdir(self.bucket):
            print(f"Path is not a directory: {self.bucket}")
            return [], []

        try:
            store = LocalStore(self.bucket)
            return self._collect_files_and_items(store, prefix)
        except Exception as e:
            print(f"obstore error: {e}")
            return [], []

    def list_s3_public(self, prefix: str = "") -> tuple[list, list]:
        """
        List all items from a public S3 path.

        Args:
            prefix: The S3 prefix to list

        Returns:
            A tuple containing a list of files and a list of unique items in the S3 path
        """
        try:
            bucket = self.bucket 
            region = self.region 
            store = S3Store(bucket=bucket, region=region, skip_signature=True)
            return self._collect_files_and_items(store, prefix)
        except Exception as e:
            print(f"Error listing public S3 path {prefix}: {e}")
            return [], []

    def list_s3_private(self, prefix: str) -> tuple[list, list]:
        """
        List all items from a private S3 path using signed credentials.

        Args:
            prefix: The S3 prefix to list
            bucket: S3 bucket name
            region: AWS region
            access_key: AWS access key ID
            secret_key: AWS secret access key
            session_token: Optional AWS session token for temporary credentials

        Returns:
            A tuple containing a list of files and a list of unique items in the S3 path
        """
        try:
            bucket = self.bucket
            region = self.region
            access_key = self.access_key
            secret_key = self.secret_key
            session_token = self.session_token
            if not (bucket and region and access_key and secret_key):
                print("Missing S3 credentials/config for private S3 access.")
                return [], []
            kwargs = {
                "bucket": bucket,
                "region": region,
                "access_key_id": access_key,
                "secret_access_key": secret_key,
            }
            if session_token:
                kwargs["token"] = session_token
            store = S3Store(**kwargs)
            return self._collect_files_and_items(store, prefix)
        except Exception as e:
            print(f"Error listing private S3 path {prefix}: {e}")
            return [], []

    def filter_files_by_extension(self, files: list, extension: str, apply_base_folder: bool = False) -> list:
        """Filter a list of file paths by a specific extension."""
        files = [f for f in files if f.endswith(extension)]
        if apply_base_folder:
            full_paths = []
            for file in files:
                file = self.bucket + "/" + file
                full_paths.append(file)
            files = full_paths
        return files
    

    def filter_paths_by_keywords(self, files: list, keywords: list, apply_base_folder: bool = False) -> list:
        """
        Filter file paths by keywords.

        Supports either:
        - A flat keyword list (legacy behavior): ["foo", "bar"]
          Matches files containing any keyword.
        - Keyword groups: [["foo", "bar"], ["baz", "qux"]]
          Matches files that satisfy any group, where all keywords in a group
          must be present in the file path.
        """
        if not keywords:
            return []

        has_groups = any(isinstance(group, list) for group in keywords)

        if has_groups:
            files = [
                f
                for f in files
                if any(all(keyword in f for keyword in group) for group in keywords)
            ]
        else:
            files = [f for f in files if any(keyword in f for keyword in keywords)]

        if apply_base_folder:
            full_paths = []
            for file in files:
                file = self.bucket + "/" + file
                full_paths.append(file)
            files = full_paths
        return files