from __future__ import annotations

"""ExecutorContext — DuckDB session wrapper.

3 standard table을 in-memory view로 등록하고, step 결과를 step_id 이름의 view에
누적한다. session은 in-memory (``:memory:``)라 plan 종료 후 자동 폐기.
"""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb

from ..sql_identifiers import SAFE_SQL_IDENTIFIER_RE


@dataclass(frozen=True)
class ArtifactPaths:
    """plan_v2 standard table을 적재할 dataset artifact 경로.

    - ``docs``: cleaned parquet (dataset_clean 출력)
    - ``clauses``: clause_label jsonl (dataset_clause_label 출력)
    - ``genuineness``: doc_genuineness jsonl (dataset_doc_genuineness 출력)
    """

    docs: Path
    clauses: Path
    genuineness: Path


class ExecutorContextError(RuntimeError):
    """artifact 경로 누락·schema 위반 등 context 초기화 단계의 오류."""


class ExecutorContext:
    """plan_v2 실행용 DuckDB session.

    ``with ExecutorContext(paths) as ctx:`` 패턴으로 사용. session은 in-memory.
    """

    def __init__(self, artifact_paths: ArtifactPaths) -> None:
        self._artifact_paths = artifact_paths
        self._con = duckdb.connect(":memory:")
        try:
            self._register_docs()
            self._validate_docs_created_at()
            self._register_clauses()
            self._register_genuineness()
        except Exception:
            self._con.close()
            raise

    # ===== lifecycle =====

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._con

    def close(self) -> None:
        self._con.close()

    def __enter__(self) -> "ExecutorContext":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ===== standard table registration =====

    def _register_docs(self) -> None:
        path = self._require_existing(self._artifact_paths.docs, "docs")
        literal = self._escape_path_literal(path)
        self._con.execute(
            f"CREATE OR REPLACE VIEW docs AS SELECT * FROM read_parquet('{literal}')"
        )

    def _register_clauses(self) -> None:
        # clause_label artifact lock에 clause_id가 없다. doc_id 기준
        # ROW_NUMBER로 deterministic하게 생성. 같은 clause 텍스트가 있는
        # 케이스는 tie 처리에 source/prompt_version까지 정렬 키에 추가해 안정화.
        path = self._require_existing(self._artifact_paths.clauses, "clauses")
        literal = self._escape_path_literal(path)
        self._con.execute(
            f"""
            CREATE OR REPLACE VIEW clauses AS
            SELECT
              doc_id,
              CAST(doc_id AS VARCHAR) || '__' || CAST(
                ROW_NUMBER() OVER (
                  PARTITION BY doc_id ORDER BY clause, source, prompt_version
                ) AS VARCHAR
              ) AS clause_id,
              clause,
              sentiment,
              aspect,
              prompt_version,
              source
            FROM read_json('{literal}', format='newline_delimited')
            """
        )

    def _register_genuineness(self) -> None:
        path = self._require_existing(self._artifact_paths.genuineness, "genuineness")
        literal = self._escape_path_literal(path)
        self._con.execute(
            f"""
            CREATE OR REPLACE VIEW genuineness AS
            SELECT
              doc_id,
              genuineness,
              reason,
              prompt_version,
              source
            FROM read_json('{literal}', format='newline_delimited')
            """
        )

    @staticmethod
    def _escape_path_literal(path: str) -> str:
        """artifact 경로를 SQL string literal로 안전하게 escape. CREATE VIEW는
        DuckDB parameter binding을 지원하지 않으므로 literal inline이 필요하다."""
        return str(path).replace("'", "''")

    def _validate_docs_created_at(self) -> None:
        column_types = self.get_column_types("docs")
        if "created_at" not in column_types:
            raise ExecutorContextError(
                "docs artifact missing required column 'created_at' — "
                "clean 단계에서 원본 날짜 컬럼을 ``created_at`` ISO timestamp로 표준화해야 함."
            )
        try:
            self._con.execute(
                "SELECT CAST(created_at AS TIMESTAMP) FROM docs WHERE created_at IS NOT NULL LIMIT 1"
            ).fetchone()
        except duckdb.Error as exc:
            raise ExecutorContextError(
                f"docs.created_at is not castable to TIMESTAMP: {exc}"
            ) from exc

    @staticmethod
    def _require_existing(path: Path, table: str) -> str:
        resolved = Path(path)
        if not resolved.exists():
            raise ExecutorContextError(f"{table} artifact missing: {resolved}")
        return str(resolved.resolve())

    # ===== step output =====

    def register_step_view(self, step_id: str, sql: str) -> None:
        """step 실행 결과를 ``step_id`` 이름의 view로 등록한다."""

        ident = self._safe_identifier(step_id)
        self._con.execute(f"CREATE OR REPLACE VIEW {ident} AS {sql}")

    def fetch_rows(self, source: str, limit: int | None = None) -> list[dict[str, Any]]:
        """view/table에서 row 목록을 dict로 가져온다. SQL identifier만 허용."""

        ident = self._safe_identifier(source)
        suffix = "" if limit is None else f" LIMIT {int(limit)}"
        result = self._con.execute(f"SELECT * FROM {ident}{suffix}")
        columns = [desc[0] for desc in result.description]
        return [dict(zip(columns, row)) for row in result.fetchall()]

    def count_rows(self, source: str) -> int:
        ident = self._safe_identifier(source)
        row = self._con.execute(f"SELECT COUNT(*) FROM {ident}").fetchone()
        return int(row[0]) if row else 0

    def get_column_types(self, source: str) -> dict[str, str]:
        ident = self._safe_identifier(source)
        rows = self._con.execute(f"DESCRIBE {ident}").fetchall()
        # DuckDB DESCRIBE: (column_name, column_type, null, key, default, extra)
        return {row[0]: row[1] for row in rows}

    def get_column_names(self, source: str) -> list[str]:
        return list(self.get_column_types(source).keys())

    # ===== internal helpers =====

    @staticmethod
    def _safe_identifier(name: str) -> str:
        if not isinstance(name, str) or not SAFE_SQL_IDENTIFIER_RE.match(name):
            raise ExecutorContextError(f"unsafe SQL identifier: {name!r}")
        return name


def read_docs_columns(artifact_paths: ArtifactPaths) -> list[str]:
    """docs view(cleaned parquet)의 실제 컬럼명을 plan 단계에서 조회한다.

    silverone 2026-06-05 — planner에 노출하는 docs-extra 컬럼을 **실제 query 가능한
    docs 컬럼**으로 거르기 위함(advertised=queryable invariant). clean이 source
    text_columns(예: 제목/본문)를 raw_text로 병합하고 나머지를 source_json에 넣으면
    그 원본 컬럼은 docs view에 없으므로, planner가 참조하면 Binder Error가 난다.
    artifact가 없으면 빈 리스트(=거르지 않음)로 degrade."""
    path = Path(artifact_paths.docs)
    if not path.exists():
        return []
    con = duckdb.connect(":memory:")
    try:
        literal = str(path.resolve()).replace("'", "''")
        rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{literal}')"
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        con.close()


__all__ = [
    "ArtifactPaths",
    "ExecutorContext",
    "ExecutorContextError",
    "read_docs_columns",
]
