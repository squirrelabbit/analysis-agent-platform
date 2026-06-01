"""5/7 결정 5-step pipeline 마이그레이션 — 기존 build artifact 폐기.

기존 5 step (clean / prepare / sentiment / embedding / cluster) 중 *clean을
제외*한 4 step의 artifact row와 ``dataset_versions.metadata``의 ``*_status``
키를 삭제한다. clean은 5-step pipeline에서도 재사용되므로 보존.

신규 5-step 흐름 (clean → segment → clause_label → embedding_cluster +
keyword_index)은 *재빌드*가 필요. 이 스크립트는 폐기 단계만 수행하고 rebuild
trigger는 운영자가 명시 호출 (silverone "destructive 신중" 정책).

⚠️  운영 환경 deploy 전 필수 백업 ⚠️
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

::

    pg_dump --no-owner --no-acl \\
        -t dataset_version_artifacts -t dataset_versions \\
        analysis_support > backup_$(date +%Y-%m-%d_%H%M).sql

또는 ::

    psql analysis_support -c \\
        "COPY dataset_version_artifacts TO '/backup/artifacts.csv' CSV HEADER"
    psql analysis_support -c \\
        "COPY dataset_versions TO '/backup/versions.csv' CSV HEADER"

복구 시 ``psql analysis_support < backup_...sql`` 또는 ``COPY ... FROM ...``.

Connection
----------
``DATABASE_URL`` 환경 변수에서 읽음. local dev 기본값 ::

    DATABASE_URL=postgresql://platform:platform@127.0.0.1:15432/analysis_support \\
        workers/python-ai/.venv/bin/python scripts/migrate_dataset_build_v2.py

옵션
----
- ``--dry-run``: 영향 받는 row 수만 출력하고 실제 삭제는 안 함 (기본 권장)
- ``--execute``: 실제 삭제 수행
- ``--project-id <uuid>``: 단일 프로젝트만 마이그레이션 (안전 운영)
"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import psycopg


_DEFAULT_DATABASE_URL = "postgresql://platform:platform@127.0.0.1:15432/analysis_support"

# 5/7 결정에서 폐기하는 artifact_type 4종. clean은 5-step에서도 재사용되므로
# 보존 (prepare/sentiment/embedding/cluster만 삭제).
_RETIRED_ARTIFACT_TYPES: tuple[str, ...] = ("prepare", "sentiment", "embedding", "cluster")

# dataset_versions.metadata에서 reset할 status 키. clean_status는 보존.
_RETIRED_METADATA_STATUS_KEYS: tuple[str, ...] = (
    "prepare_status",
    "sentiment_status",
    "embedding_status",
    "cluster_status",
    "prepare_uri",
    "prepared_ref",
    "sentiment_uri",
    "sentiment_ref",
    "embedding_uri",
    "embedding_index_source_ref",
    "cluster_uri",
    "cluster_membership_ref",
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="5-step pipeline 마이그레이션 — 기존 prepare/sentiment/embedding/cluster artifact 폐기",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="실제 삭제 안 함 — 영향 row 수만 출력 (기본값)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="실제 삭제 수행 (백업 후에만)",
    )
    parser.add_argument(
        "--project-id",
        default="",
        help="단일 프로젝트로 제한 (옵션, 운영 환경에서 안전)",
    )
    args = parser.parse_args()

    if args.execute:
        args.dry_run = False

    database_url = os.environ.get("DATABASE_URL", _DEFAULT_DATABASE_URL).strip()
    if not database_url:
        print("DATABASE_URL is required", file=sys.stderr)
        return 2

    project_filter = args.project_id.strip()

    artifact_filter_sql = "artifact_type = ANY(%s)"
    artifact_filter_params: list[Any] = [list(_RETIRED_ARTIFACT_TYPES)]
    if project_filter:
        artifact_filter_sql += " AND project_id = %s::uuid"
        artifact_filter_params.append(project_filter)

    print(f"=== dataset_build v2 migration ({'DRY-RUN' if args.dry_run else 'EXECUTE'}) ===")
    if project_filter:
        print(f"project filter: {project_filter}")
    print(f"retired artifact_types: {_RETIRED_ARTIFACT_TYPES}")
    print(f"retired metadata keys: {_RETIRED_METADATA_STATUS_KEYS}")

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM dataset_version_artifacts WHERE {artifact_filter_sql}",
                artifact_filter_params,
            )
            artifact_count = cur.fetchone()[0]
            print(f"affected dataset_version_artifacts rows: {artifact_count}")

            version_filter_sql = "TRUE"
            version_filter_params: list[Any] = []
            if project_filter:
                version_filter_sql = "project_id = %s::uuid"
                version_filter_params = [project_filter]
            cur.execute(
                f"""SELECT COUNT(*) FROM dataset_versions
                    WHERE {version_filter_sql}
                      AND metadata ?| %s::text[]""",
                version_filter_params + [list(_RETIRED_METADATA_STATUS_KEYS)],
            )
            version_count = cur.fetchone()[0]
            print(f"affected dataset_versions rows (metadata reset): {version_count}")

            if args.dry_run:
                print("dry-run — no changes applied. Use --execute (after backup) to apply.")
                return 0

            cur.execute(
                f"DELETE FROM dataset_version_artifacts WHERE {artifact_filter_sql}",
                artifact_filter_params,
            )
            print(f"deleted dataset_version_artifacts rows: {cur.rowcount}")

            for key in _RETIRED_METADATA_STATUS_KEYS:
                cur.execute(
                    f"""UPDATE dataset_versions
                        SET metadata = metadata - %s
                        WHERE {version_filter_sql}
                          AND metadata ? %s""",
                    version_filter_params + [key, key],
                )
                print(f"  - removed metadata key {key!r}: {cur.rowcount} rows")

            cur.execute(
                f"""UPDATE dataset_versions
                    SET prepare_status = '',
                        sentiment_status = '',
                        embedding_status = ''
                    WHERE {version_filter_sql}
                      AND (prepare_status <> '' OR sentiment_status <> '' OR embedding_status <> '')""",
                version_filter_params,
            )
            print(f"reset prepare_status/sentiment_status/embedding_status columns: {cur.rowcount} rows")

        conn.commit()
    print("=== migration complete ===")
    print("후속: 운영자가 신규 5-step build를 명시 호출 (silverone 정책 — destructive trigger 회피).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
