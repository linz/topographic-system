import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import importlib.util

import pytest  # type: ignore

# Setup path for importing postgis_manage_fields module
_module_path = (
    Path(__file__).parents[1]
    / "nztopo50"
    / "import"
    / "core"
    / "postgis_manage_fields.py"
)
_core_path = Path(__file__).parents[1] / "nztopo50" / "import" / "core"
sys.path.insert(0, str(_core_path))


@pytest.fixture
def mock_connection():
    """Create a mock psycopg connection and cursor."""
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.closed = False

    return mock_conn, mock_cursor


@pytest.fixture
def mock_psycopg(mock_connection):
    """Patch psycopg module before importing the class."""
    mock_conn, mock_cursor = mock_connection
    mock_psycopg_module = SimpleNamespace(connect=MagicMock(return_value=mock_conn))
    with patch.dict(sys.modules, {"psycopg": mock_psycopg_module}):
        # Import happens dynamically inside patch context to avoid psycopg import errors
        from postgis_manage_fields import ModifyTable  # type: ignore[import]

        yield ModifyTable, mock_conn, mock_cursor


def test_modify_table_initialization(mock_psycopg):
    """Test ModifyTable initializes with db_params."""
    ModifyTable, mock_conn, _ = mock_psycopg
    db_params = {
        "dbname": "test_db",
        "user": "testuser",
        "password": "testpass",
        "host": "localhost",
        "port": 5432,
    }

    modifier = ModifyTable(db_params)

    assert modifier.conn == mock_conn
    assert modifier.db_params == db_params


def test_table_exists_returns_true_when_found(mock_psycopg):
    """Test table_exists returns True when table is found."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = (1,)

    modifier = ModifyTable({})
    result = modifier.table_exists("public", "users")

    assert result is True
    mock_cursor.execute.assert_called()


def test_table_exists_returns_false_when_not_found(mock_psycopg):
    """Test table_exists returns False when table not found."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = None

    modifier = ModifyTable({})
    result = modifier.table_exists("public", "nonexistent")

    assert result is False


def test_column_exists_returns_true_when_found(mock_psycopg):
    """Test column_exists returns True when column is found."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = (1,)

    modifier = ModifyTable({})
    result = modifier.column_exists("public", "users", "email")

    assert result is True


def test_column_exists_with_like_pattern(mock_psycopg):
    """Test column_exists uses LIKE pattern when use_like=True."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = (1,)

    modifier = ModifyTable({})
    result = modifier.column_exists("public", "users", "use", use_like=True)

    assert result is True
    # Check that LIKE was used in the query
    call_args = mock_cursor.execute.call_args[0][0]
    assert "like '%use%'" in call_args


def test_column_list_returns_matching_columns(mock_psycopg):
    """Test column_list returns columns matching substring pattern."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchall.return_value = [("feature_type",), ("feature_use",)]

    modifier = ModifyTable({})
    result = modifier.column_list("public", "features", "feature")

    assert result == ["feature_type", "feature_use"]


def test_list_schema_tables_returns_grouped_tables(mock_psycopg):
    """Test list_schema_tables returns dict of schema->tables mapping."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchall.return_value = [
        ("public", "users"),
        ("public", "products"),
    ]

    modifier = ModifyTable({})
    result = modifier.list_schema_tables("public")

    assert "public" in result
    assert "users" in result["public"]
    assert "products" in result["public"]


def test_add_column_without_default(mock_psycopg):
    """Test add_column appends DEFAULT NULL when data_type has no DEFAULT."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.add_column("public.users", "status", "VARCHAR(50)")

    sql = mock_cursor.execute.call_args[0][0]
    assert 'ALTER TABLE public.users ADD COLUMN IF NOT EXISTS "status"' in sql
    assert "VARCHAR(50) DEFAULT NULL" in sql


def test_add_column_with_default(mock_psycopg):
    """Test add_column uses explicit DEFAULT from data_type."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.add_column("public.users", "created_at", "DATE DEFAULT CURRENT_DATE")

    sql = mock_cursor.execute.call_args[0][0]
    assert "DEFAULT CURRENT_DATE" in sql
    # Should NOT add DEFAULT NULL when DEFAULT is already present
    assert not sql.count("DEFAULT") > 1


def test_get_srid_returns_value(mock_psycopg):
    """Test get_srid returns SRID value from query result."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = (2193,)

    modifier = ModifyTable({})
    result = modifier.get_srid("public", "features", "geom")

    assert result == 2193


def test_get_srid_returns_none_when_not_found(mock_psycopg):
    """Test get_srid returns None when no result found."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = None

    modifier = ModifyTable({})
    result = modifier.get_srid("public", "features", "geom")

    assert result is None


def test_get_geometry_type_returns_type(mock_psycopg):
    """Test get_geometry_type returns geometry type string."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = ("POLYGON",)

    modifier = ModifyTable({})
    result = modifier.get_geometry_type("public", "features")

    assert result == "POLYGON"


def test_rename_columns_when_column_exists(mock_psycopg):
    """Test rename_columns executes rename when column exists."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    # First call: column_exists check returns True
    mock_cursor.fetchone.return_value = (1,)

    modifier = ModifyTable({})
    modifier.rename_columns("public", "features", "old_name", "new_name")

    # Should have called execute twice: once for column_exists, once for rename
    assert mock_cursor.execute.call_count >= 2


def test_rename_columns_skips_when_column_missing(mock_psycopg):
    """Test rename_columns skips when column doesn't exist."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = None

    modifier = ModifyTable({})
    modifier.rename_columns("public", "features", "missing_col", "new_name")

    # Should query for column existence but not issue rename
    execute_calls = mock_cursor.execute.call_args_list
    # Check that no ALTER TABLE RENAME was called
    rename_called = any("RENAME COLUMN" in str(call) for call in execute_calls)
    assert not rename_called


def test_drop_column_when_exists(mock_psycopg):
    """Test drop_column executes DROP when column exists."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg
    mock_cursor.fetchone.return_value = (1,)

    modifier = ModifyTable({})
    modifier.drop_column("public", "features", "unused_col")

    execute_calls = mock_cursor.execute.call_args_list
    drop_called = any("DROP COLUMN" in str(call) for call in execute_calls)
    assert drop_called


def test_update_column_with_default_without_where(mock_psycopg):
    """Test update_column_with_default without WHERE clause."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.update_column_with_default("public", "features", "status", "'active'")

    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE public.features SET status = 'active'" in sql
    assert "WHERE" not in sql


def test_update_column_with_default_with_where(mock_psycopg):
    """Test update_column_with_default with WHERE clause."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.update_column_with_default(
        "public", "features", "status", "'active'", where_clause="type = 'road'"
    )

    sql = mock_cursor.execute.call_args[0][0]
    assert "UPDATE public.features SET status = 'active'" in sql
    assert "WHERE type = 'road'" in sql


def test_update_primary_key_creates_sequence(mock_psycopg):
    """Test update_primary_key creates sequence and sets primary key."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.update_primary_key("public", "features", "id")

    execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

    # Should create sequence
    assert any("CREATE SEQUENCE" in sql for sql in execute_calls)
    # Should add primary key constraint
    assert any("ADD PRIMARY KEY" in sql for sql in execute_calls)


def test_update_primary_key_guid_uses_gen_random_uuid(mock_psycopg):
    """Test update_primary_key_guid sets default to gen_random_uuid()."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.update_primary_key_guid("public", "features", "topo_id")

    execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]

    # Should set default to gen_random_uuid()
    assert any("gen_random_uuid()" in sql for sql in execute_calls)
    # Should add primary key constraint
    assert any("ADD PRIMARY KEY" in sql for sql in execute_calls)


def test_carto_text_geom_update_snaps_to_grid(mock_psycopg):
    """Test carto_text_geom_update uses ST_SnapToGrid."""
    ModifyTable, mock_conn, mock_cursor = mock_psycopg

    modifier = ModifyTable({})
    modifier.carto_text_geom_update("public", "map_sheet")

    sql = mock_cursor.execute.call_args[0][0]
    assert "ST_SnapToGrid(geometry, 1.0)" in sql


def test_all_ordered_columns_default_keeps_id(mock_psycopg):
    """Test all_ordered_columns keeps 'id' when primary_key_type != 'uuid'."""
    ModifyTable, mock_conn, _ = mock_psycopg

    modifier = ModifyTable({})
    columns = modifier.all_ordered_columns(primary_key_type="int")

    assert "id" in columns
    assert "topo_id" in columns


def test_all_ordered_columns_uuid_moves_topo_id_first(mock_psycopg):
    """Test all_ordered_columns reorders for UUID primary key."""
    ModifyTable, mock_conn, _ = mock_psycopg

    modifier = ModifyTable({})
    columns = modifier.all_ordered_columns(primary_key_type="uuid")

    assert "id" not in columns
    assert columns[0] == "topo_id"


def test_table_modification_workflow_initialization(mock_psycopg):
    """Test TableModificationWorkflow initializes with config."""
    from postgis_manage_fields import TableModificationWorkflow  # type: ignore[import]

    workflow = TableModificationWorkflow(
        db_params={"dbname": "test"},
        schema_name="release64",
        option="all",
        add_full_metadata_fields=True,
        primary_key_type="uuid",
        release_date="2025-09-25",
    )

    assert workflow.schema_name == "release64"
    assert workflow.option == "all"
    assert workflow.primary_key_type == "uuid"


def test_table_modification_workflow_should_run_all(mock_psycopg):
    """Test should_run returns True for all steps when option='all'."""
    from postgis_manage_fields import TableModificationWorkflow  # type: ignore[import]

    workflow = TableModificationWorkflow(
        db_params={},
        schema_name="release64",
        option="all",
    )

    assert workflow.should_run("metadata") is True
    assert workflow.should_run("columns") is True
    assert workflow.should_run("defaults") is True


def test_table_modification_workflow_should_run_specific_step(mock_psycopg):
    """Test should_run matches specific steps."""
    from postgis_manage_fields import TableModificationWorkflow  # type: ignore[import]

    workflow = TableModificationWorkflow(
        db_params={},
        schema_name="release64",
        option="metadata",
    )

    assert workflow.should_run("metadata") is True
    assert workflow.should_run("columns") is False
    assert workflow.should_run("defaults") is False
