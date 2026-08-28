"""
Data contracts + producer auto-registration for dataset sources.

This is a utility module, not a DAG file.

A *contract* is a declarative description (one YAML per shared table shape under
``airflow/dags/contracts/``) of the MINIMUM columns a producer table must expose
for downstream consumers to work. Producer DAGs call :func:`register_source` as
their final task: it validates each produced table against its contract and, on
success, upserts a row into the ``data_sources`` registry. Consumers then iterate
the registry instead of hardcoding per-source table names.

Only Airflow DAGs import this module. The API does NOT import it; it reads the
``data_sources`` table directly via SQL.

Cursor contract: every function takes a standard (tuple-returning) DB-API cursor,
matching the cursors used elsewhere in the DAG code (e.g. ``create_unified_datasets_view``).
"""

from pathlib import Path
from typing import Dict, List, Optional

import yaml
from pydantic import BaseModel

# Contracts live next to the DAGs so they are available inside the mounted
# /opt/airflow/dags tree at runtime (docker-compose only mounts airflow/dags).
CONTRACTS_DIR = Path(__file__).resolve().parent.parent / "contracts"


class ColumnSpec(BaseModel):
    name: str
    type: str
    nullable: bool = True


class TableContract(BaseModel):
    name: str
    description: str = ""
    columns: List[ColumnSpec]


# Fold postgres type spellings to a canonical token so contracts can use the
# friendly names while we compare against information_schema.columns.udt_name
# (which already collapses e.g. "timestamp with time zone" -> "timestamptz").
_TYPE_CANON = {
    "varchar": "varchar",
    "character varying": "varchar",
    "text": "text",
    "int": "integer",
    "int4": "integer",
    "integer": "integer",
    "int8": "bigint",
    "bigint": "bigint",
    "bool": "boolean",
    "boolean": "boolean",
    "jsonb": "jsonb",
    "json": "json",
    "timestamp": "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamptz": "timestamptz",
    "timestamp with time zone": "timestamptz",
    "date": "date",
    "numeric": "numeric",
    "float8": "double precision",
    "double precision": "double precision",
}


def _canon_type(t: str) -> str:
    return _TYPE_CANON.get(t.strip().lower(), t.strip().lower())


def _subst(col_name: str, source_prefix: Optional[str]) -> str:
    """Substitute the ``{source}`` placeholder with the per-source prefix."""
    if "{source}" in col_name:
        if not source_prefix:
            raise ValueError(
                f"Contract column '{col_name}' uses a {{source}} placeholder "
                "but no source_prefix was provided"
            )
        return col_name.replace("{source}", source_prefix)
    return col_name


def load_contract(name: str) -> TableContract:
    """Load and parse a contract YAML by name (without extension)."""
    path = CONTRACTS_DIR / f"{name}.yml"
    if not path.exists():
        raise FileNotFoundError(f"Contract '{name}' not found at {path}")
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    return TableContract.model_validate(data)


def ensure_registry_table(cursor) -> None:
    """Create the data_sources registry table if it does not exist (idempotent).

    This DDL mirrors database/init-db.sql, the canonical definition for fresh
    database initialization. If the schema changes, update both files.
    """
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS data_sources (
            source_name   TEXT NOT NULL,
            contract_name TEXT NOT NULL,
            table_name    TEXT NOT NULL,
            source_id_col TEXT,
            registered_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (source_name, contract_name)
        );
        """
    )


def validate_table(
    cursor,
    *,
    table: str,
    contract: TableContract,
    source_prefix: Optional[str] = None,
) -> None:
    """
    Assert that ``table`` satisfies ``contract``.

    Raises ValueError listing any missing columns or incompatible column types.
    Extra columns on the table are allowed and ignored. Checks presence + type
    only (not NOT-NULL) to avoid false failures on legacy tables.
    """
    cursor.execute(
        """
        SELECT column_name, udt_name
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        """,
        (table,),
    )
    actual = {row[0]: row[1] for row in cursor.fetchall()}
    if not actual:
        raise ValueError(
            f"Contract '{contract.name}': table '{table}' does not exist "
            "(or has no columns) in schema 'public'"
        )

    missing: List[str] = []
    mismatches: List[str] = []
    for col in contract.columns:
        real_name = _subst(col.name, source_prefix)
        if real_name not in actual:
            missing.append(real_name)
            continue
        expected = _canon_type(col.type)
        got = _canon_type(actual[real_name])
        if expected != got:
            mismatches.append(
                f"{real_name} (contract={col.type}->{expected}, actual={actual[real_name]}->{got})"
            )

    if missing or mismatches:
        problems = []
        if missing:
            problems.append(f"missing columns: {', '.join(missing)}")
        if mismatches:
            problems.append(f"type mismatches: {'; '.join(mismatches)}")
        raise ValueError(
            f"Contract '{contract.name}' validation failed for table '{table}': "
            + " | ".join(problems)
        )


def register_source(
    cursor,
    *,
    source_name: str,
    tables: Dict[str, str],
    source_prefix: Optional[str] = None,
) -> None:
    """
    Validate each ``{contract_name: table_name}`` entry and, on success, upsert a
    row per (source_name, contract_name) into the data_sources registry.

    All tables are validated BEFORE any registry row is written, so a partially
    broken producer leaves the registry untouched (atomic within the caller's
    transaction). ``source_prefix`` (e.g. "dandi") drives both the {source}
    column substitution and the stored ``source_id_col``.
    """
    ensure_registry_table(cursor)

    # Validate everything first — never register a source whose tables drifted.
    for contract_name, table_name in tables.items():
        contract = load_contract(contract_name)
        validate_table(
            cursor, table=table_name, contract=contract, source_prefix=source_prefix
        )

    source_id_col = f"{source_prefix}_id" if source_prefix else None
    for contract_name, table_name in tables.items():
        cursor.execute(
            """
            INSERT INTO data_sources
                (source_name, contract_name, table_name, source_id_col, registered_at)
            VALUES (%s, %s, %s, %s, now())
            ON CONFLICT (source_name, contract_name)
            DO UPDATE SET
                table_name    = EXCLUDED.table_name,
                source_id_col = EXCLUDED.source_id_col,
                registered_at = now();
            """,
            (source_name, contract_name, table_name, source_id_col),
        )


def list_registered_sources(
    cursor, *, contract_name: Optional[str] = None
) -> List[Dict[str, Optional[str]]]:
    """
    Canonical registry reader for consumers. Returns a list of dicts with keys
    source_name, contract_name, table_name, source_id_col.

    Calls ensure_registry_table first so readers tolerate the table being absent
    (returns an empty list), not just empty.
    """
    ensure_registry_table(cursor)
    if contract_name:
        cursor.execute(
            """
            SELECT source_name, contract_name, table_name, source_id_col
            FROM data_sources
            WHERE contract_name = %s
            ORDER BY source_name
            """,
            (contract_name,),
        )
    else:
        cursor.execute(
            """
            SELECT source_name, contract_name, table_name, source_id_col
            FROM data_sources
            ORDER BY source_name, contract_name
            """
        )
    return [
        {
            "source_name": row[0],
            "contract_name": row[1],
            "table_name": row[2],
            "source_id_col": row[3],
        }
        for row in cursor.fetchall()
    ]
