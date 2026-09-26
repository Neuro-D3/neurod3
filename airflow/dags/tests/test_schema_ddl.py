"""
Tests for utils.database.apply_schema_ddl: start-up DDL must skip statements
that would change nothing, because `ADD COLUMN IF NOT EXISTS` and
`CREATE INDEX IF NOT EXISTS` lock the table even then, and overlapping DAG runs
deadlocked on those locks. No database: a fake cursor records what ran.
"""

from utils.database import apply_schema_ddl, split_sql_statements


class FakeCursor:
    def __init__(self, columns=None, indexes=()):
        self.columns = {t: set(c) for t, c in (columns or {}).items()}
        self.indexes = set(indexes)
        self.ran = []
        self._result = None

    def execute(self, sql, args=()):
        s = " ".join(sql.split())
        if s.startswith("SELECT column_name FROM information_schema.columns"):
            self._result = [(c,) for c in self.columns.get(args[0], set())]
        elif s.startswith("SELECT to_regclass"):
            self._result = [(args[0].split(".", 1)[1] in self.indexes,)]
        else:
            self.ran.append(s)

    def fetchall(self):
        return self._result

    def fetchone(self):
        return self._result[0] if self._result else None


DDL = """
-- papers columns
CREATE TABLE IF NOT EXISTS papers (paper_doi TEXT PRIMARY KEY);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS title TEXT;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS journal TEXT;  -- newer
CREATE INDEX IF NOT EXISTS idx_papers_title ON papers(title);
CREATE OR REPLACE VIEW v AS SELECT 1;
"""


def test_split_drops_comments_and_blank_statements():
    stmts = split_sql_statements(DDL)
    assert len(stmts) == 5
    assert not any("--" in s for s in stmts)


def test_everything_present_skips_the_locking_statements():
    cur = FakeCursor(columns={"papers": {"paper_doi", "title", "journal"}}, indexes={"idx_papers_title"})
    res = apply_schema_ddl(cur, DDL)
    assert res == {"ran": 2, "skipped": 3}
    assert not any(s.startswith(("ALTER", "CREATE INDEX")) for s in cur.ran)
    # CREATE TABLE IF NOT EXISTS and views still run: they take no data-table lock.
    assert cur.ran[0].startswith("CREATE TABLE IF NOT EXISTS papers")


def test_missing_column_and_index_still_run():
    cur = FakeCursor(columns={"papers": {"paper_doi", "title"}}, indexes=set())
    res = apply_schema_ddl(cur, DDL)
    assert "ALTER TABLE papers ADD COLUMN IF NOT EXISTS journal TEXT" in cur.ran
    assert "ALTER TABLE papers ADD COLUMN IF NOT EXISTS title TEXT" not in cur.ran
    assert any(s.startswith("CREATE INDEX IF NOT EXISTS idx_papers_title") for s in cur.ran)
    assert res["skipped"] == 1


def test_alter_that_does_more_than_add_columns_always_runs():
    cur = FakeCursor(columns={"t": {"a"}})
    apply_schema_ddl(cur, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT, ALTER COLUMN a SET DEFAULT 0;")
    assert len(cur.ran) == 1


def test_multi_column_alter_skipped_only_when_all_exist():
    cur = FakeCursor(columns={"t": {"a"}})
    apply_schema_ddl(cur, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT, ADD COLUMN IF NOT EXISTS b INT;")
    assert len(cur.ran) == 1
    cur = FakeCursor(columns={"t": {"a", "b"}})
    apply_schema_ddl(cur, "ALTER TABLE t ADD COLUMN IF NOT EXISTS a INT, ADD COLUMN IF NOT EXISTS b INT;")
    assert cur.ran == []
