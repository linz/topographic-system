"""Gives a dataset's Kart schema stable column ids across independent pipeline runs.

`kart import --replace-existing` mints a fresh random uuid4 for every column whenever there's no
existing dataset to align the new schema against (Kart's OGR import path, `ColumnSchema.new_id()`),
so two independent imports of identical data -- e.g. two full rebuilds of the same theme, which
each start from a freshly deleted and reinitialised repo -- never produce matching trees. Kart
stores each feature keyed partly by its schema's column ids, so a rebuild-to-rebuild id change
makes `kart diff`/`kart log` show "entire dataset replaced" against a previous bundle instead of
the real, small change -- defeating the point of using Kart at all.

`assign_stable_column_ids` fixes this by rewriting just the `id` field of each column in a
dataset's `schema.json` to a uuid5 derived deterministically from the theme and column names, via
plumbing (no `kart` CLI call). Everything else `kart import` already inferred correctly --
`dataType`, `geometryType`, `size`, etc. -- is left untouched.

The dataset's `meta/legend/<hash>` blob is deliberately *not* touched or regenerated: it is read
only when decoding a real feature blob (`TableV3.get_raw_feature_dict`), and this is only ever
applied to a *schema-only* stub commit with zero features, so nothing ever reads it. A later real
`kart import --replace-existing` realigns purely against `schema.json` (`Schema.align_to_self`)
and writes its own fresh, correct legend for whatever it actually commits -- verified empirically
against a real Kart repo before relying on it here.
"""

import json
import tempfile
import uuid
from pathlib import Path

from ..command import run_command

# Arbitrary, but fixed forever: changing this would change every column id on the next rebuild.
_NAMESPACE = uuid.UUID("57730ec2-ae17-4db6-9221-4b891d34ce05")


def _stable_column_id(theme_name: str, column_name: str) -> str:
    return str(uuid.uuid5(_NAMESPACE, f"{theme_name}:{column_name}"))


def assign_stable_column_ids(repo_dir: Path, commit: str, theme_name: str) -> str:
    """Returns a new commit: `commit`, but with `<theme_name>/.table-dataset/meta/schema.json`'s
    column ids replaced by deterministic ones. Every other file in the tree is untouched.
    """
    schema_path = f"{theme_name}/.table-dataset/meta/schema.json"

    content = run_command(["git", "show", f"{commit}:{schema_path}"], cwd=str(repo_dir))
    columns = json.loads(content)
    for column in columns:
        column["id"] = _stable_column_id(theme_name, column["name"])

    with tempfile.TemporaryDirectory() as scratch:
        scratch_dir = Path(scratch)

        new_blob_file = scratch_dir / "schema.json"
        new_blob_file.write_text(json.dumps(columns))
        new_blob = run_command(["git", "hash-object", "-w", str(new_blob_file)], cwd=str(repo_dir)).strip()

        # Splice just this one blob into the tree via a scratch index: `write-tree` rebuilds only
        # the intermediate trees the change actually touches, so this is the whole tree edit
        # without hand-building any tree level ourselves.
        index_env = {"GIT_INDEX_FILE": str(scratch_dir / "index")}
        run_command(["git", "read-tree", commit], cwd=str(repo_dir), env=index_env)
        run_command(
            ["git", "update-index", "--add", "--cacheinfo", f"100644,{new_blob},{schema_path}"],
            cwd=str(repo_dir),
            env=index_env,
        )
        new_tree = run_command(["git", "write-tree"], cwd=str(repo_dir), env=index_env).strip()

    return run_command(["git", "commit-tree", new_tree, "-m", "stable column ids"], cwd=str(repo_dir)).strip()
