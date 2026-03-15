import logging
from pathlib import Path

import pytest

try:
    from analyzers.sql_lineage import SQLLineageAnalyzer
except ModuleNotFoundError:
    pytest.skip("sqlglot not installed; install dependencies before running SQL lineage tests", allow_module_level=True)


def write_sql(tmp_path, name, content):
    path = tmp_path / name
    path.write_text(content)
    return path


def test_analyze_simple_sql(tmp_path):
    """Happy path: tables from FROM/JOIN statements are captured."""
    sql = """
    with users as (
        select id from raw.users
    )

    select u.id, o.total
    from users u
    join raw.orders o on u.id = o.user_id
    """
    path = write_sql(tmp_path, "simple.sql", sql)

    analyzer = SQLLineageAnalyzer()
    deps = analyzer.analyze_file(path)
    assert deps
    target_names = [dep.target for dep in deps]
    assert "users" in target_names
    assert any("raw.users" in src for dep in deps for src in dep.sources)


def test_invalid_sql_logs_warning(tmp_path, caplog):
    """Invalid SQL should log a warning but not raise."""
    path = write_sql(tmp_path, "invalid.sql", "select from missing;")
    analyzer = SQLLineageAnalyzer()

    caplog.set_level(logging.WARNING)
    deps = analyzer.analyze_file(path)

    assert deps == []
    assert "Could not parse SQL" in caplog.text


def test_dbt_macro_fallback_produces_dependencies(tmp_path, caplog):
    sql = """
    with source as (
        select * from {{ source('raw', 'orders') }}
    )
    select * from source
    """
    path = write_sql(tmp_path, "stg_orders.sql", sql)
    analyzer = SQLLineageAnalyzer()
    caplog.set_level(logging.WARNING)
    deps = analyzer.analyze_file(path)
    assert deps
    assert deps[0].target == "stg_orders"
    assert "raw.orders" in deps[0].sources
    # Fallback should avoid warning spam for templated SQL.
    assert "Could not parse SQL" not in caplog.text
