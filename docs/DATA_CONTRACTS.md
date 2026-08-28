# Data Contracts and the Source Registry

Adding a new dataset source used to mean editing a hardcoded list in four
different places: the `unified_datasets` view builder, the paper-reuse
classifier, and two spots in the API. Miss one and the source silently
disappeared from part of the platform.

This document describes the contract + registry mechanism that replaces those
hardcoded lists. The short version: **producer DAGs declare what they produce,
consumers read that declaration.**

---

## Concepts

### A contract

A *contract* is a YAML file describing the **minimum** columns a producer table
must expose for downstream consumers to work. Contracts live in
[`airflow/dags/contracts/`](../airflow/dags/contracts/) — next to the DAGs, so
they land inside the mounted `/opt/airflow/dags` tree at runtime.

There is one contract per *shared table shape*, not one per source. All four
ingestion DAGs produce a table matching `dataset_table`; all four paper-mapping
DAGs produce tables matching the other three.

| Contract | Produced by | Example table |
|---|---|---|
| `dataset_table` | `*_ingestion` DAGs | `dandi_dataset` |
| `paper_map_table` | `*_paper_mapping` DAGs | `dandi_paper_map` |
| `paper_citations_table` | `*_paper_mapping` DAGs | `dandi_paper_citations` |
| `paper_classifications_table` | `*_paper_mapping` DAGs | `dandi_paper_citation_classifications` |

Extra columns on the real table are **allowed and ignored**. A contract is a
floor, not a schema definition — `dandi_dataset` carries `version`,
`openneuro_dataset` carries download counts, and neither breaks the contract.

### The registry

`data_sources` is a table in `dag_data` holding one row per
`(source_name, contract_name)`:

| Column | Meaning |
|---|---|
| `source_name` | Display label, e.g. `DANDI`, `OpenNeuro` |
| `contract_name` | Which contract this table satisfies |
| `table_name` | The actual table, e.g. `dandi_paper_map` |
| `source_id_col` | The source-prefixed id column, e.g. `dandi_id` (NULL for `dataset_table`) |
| `registered_at` | Last successful registration |

It is defined in two places that must stay in sync:
[`database/init-db.sql`](../database/init-db.sql) (canonical, for fresh
databases) and `utils.contracts.ensure_registry_table` (idempotent runtime
creation, so existing databases self-heal without re-running the init script).

---

## How producers register

Every producer DAG ends with a `register_*` task that calls `register_source`.
It runs **last**, so it only executes if the ingestion or mapping upstream of it
succeeded — a broken producer never enters the registry.

```python
from utils.contracts import register_source

def register_dandi_paper_sources(**context):
    with get_db_connection() as conn:
        cursor = conn.cursor()
        register_source(
            cursor,
            source_name="DANDI",
            source_prefix="dandi",
            tables={
                "paper_map_table": "dandi_paper_map",
                "paper_citations_table": "dandi_paper_citations",
                "paper_classifications_table": "dandi_paper_citation_classifications",
            },
        )
        conn.commit()
```

`register_source` guarantees two things:

- **Validate-all-then-write.** Every table is checked against its contract
  *before* any registry row is written, so a partially broken producer leaves
  the registry untouched.
- **Idempotent.** Rows are upserted on `(source_name, contract_name)`. Re-running
  a DAG refreshes `registered_at` and nothing else.

### The `{source}` placeholder

Per-source paper tables use source-prefixed column names, so the contracts
declare them as templates:

```yaml
columns:
  - {name: "{source}_id", type: varchar, nullable: false}
  - {name: "{source}_title", type: text}
```

`source_prefix="dandi"` substitutes these to `dandi_id` / `dandi_title` at
validation time, and is also what gets stored as `source_id_col`. A contract
with a `{source}` placeholder raises if you register it without a prefix.

`dataset_table` uses constant column names (`dataset_id`, `title`) and needs no
prefix — which is why its registry rows have a NULL `source_id_col`.

---

## How consumers read the registry

Three consumers replaced hardcoded per-source lists:

**`utils.database.create_unified_datasets_view`** merges the canonical bootstrap
sources with anything registered under `dataset_table`, then emits one `SELECT`
branch per source whose table actually exists.

**`paper_reuse_classification`** resolves `(internal_key, citations_table,
classifications_table, id_col)` rows by joining the registry's
`paper_citations_table` and `paper_classifications_table` entries.

**`api/main.py`** reads `data_sources` with **plain SQL** rather than importing
`utils.contracts` — the API container does not mount the Airflow dags tree. It
uses the registry to build the paper-mapping CTEs and to derive the valid
`?source=` allowlist.

### Fallback behavior

Every consumer degrades to the canonical `DANDI` / `OpenNeuro` / `CRCNS` /
`SPARC` list when the registry is absent or empty. This matters during rollout:
existing deployments have the producer tables but no `data_sources` rows until
each DAG runs once. Nothing breaks in that window.

Note that **table existence, not registration, is the inclusion signal** for the
unified view. Every ingestion DAG rebuilds the view *before* it registers
itself, so gating on registration would drop a source from its own run.

---

## Adding a new source

This is the payoff. A new source needs **no edits to the view builder, the
classifier, or the API**:

1. Write the ingestion DAG. Produce a table satisfying `dataset_table`.
2. Add a final task calling `register_source(cursor, source_name="MySource",
   tables={"dataset_table": "mysource_dataset"})`.
3. Run the DAG.

The source now appears in `unified_datasets`, is accepted by
`GET /api/datasets?source=MySource`, and shows up in
`GET /api/datasets/stats`.

For paper mapping, do the same with the other three contracts and a
`source_prefix`, and the classifier picks it up as well.

---

## Type checking

`validate_table` compares each contract column against
`information_schema.columns.udt_name`, after folding both sides through a
canonicalization table so friendly spellings work (`int` / `int4` / `integer`
all mean `integer`; `timestamp with time zone` means `timestamptz`).

It checks **presence and type only** — not `NOT NULL` — to avoid false failures
on legacy tables that predate the contract.

### Gotcha: `timestamp` is not `timestamptz`

These are genuinely different Postgres types and the validator treats them as
such. All four `*_dataset` tables (and `neuroscience_datasets`) declare
`created_at` / `updated_at` as plain `TIMESTAMP`, so `dataset_table` specifies
`timestamp`. The per-source *paper* tables use `TIMESTAMPTZ`, so those three
contracts specify `timestamptz`.

If you add a column to a contract, check the producer's actual DDL rather than
assuming. A mismatch fails the `register_*` task — loudly and on every run,
which is the intended behavior, but it will block the DAG.

---

## Operational notes

- **Reading the registry creates it.** `list_registered_sources` calls
  `ensure_registry_table` first so readers tolerate the table being absent
  rather than erroring. This means a read path can issue `CREATE TABLE IF NOT
  EXISTS` as a side effect.
- **The API caches the source allowlist for 60 seconds.** It is consulted on
  every `/api/datasets*` request, so a short TTL avoids a round-trip per call.
  A newly registered source becomes visible within one TTL.
- **The API allowlist unions legacy sources.** `Kaggle` and `PhysioNet` live
  only in `neuroscience_datasets` and have no producer DAG, so the allowlist is
  registry sources ∪ `SELECT DISTINCT source FROM neuroscience_datasets`.

## Inspecting the registry

```bash
docker compose exec postgres psql -U airflow -d dag_data -c "SELECT * FROM data_sources ORDER BY source_name, contract_name;"
```

A healthy deployment with all eight DAGs run shows 16 rows — four sources times
four contracts.
