#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
REPO_ROOT=$(cd "${SCRIPT_DIR}/../../.." && pwd)
COMPOSE_FILE="${REPO_ROOT}/compose.dev.yml"
PROJECT_NAME="${COMPOSE_PROJECT_NAME:-$(basename "${REPO_ROOT}")}"
DEFAULT_VOLUME_NAME="${PROJECT_NAME}_postgres_dev_data"

find_postgres_volume() {
  local labeled_volume
  labeled_volume=$(
    docker volume ls \
      --filter "label=com.docker.compose.project=${PROJECT_NAME}" \
      --filter "label=com.docker.compose.volume=postgres_dev_data" \
      --format '{{.Name}}'
  )
  if [[ -n "${labeled_volume}" ]]; then
    printf '%s\n' "${labeled_volume}" | head -n 1
    return 0
  fi
  if docker volume inspect "${DEFAULT_VOLUME_NAME}" >/dev/null 2>&1; then
    printf '%s\n' "${DEFAULT_VOLUME_NAME}"
    return 0
  fi
  return 1
}

echo "[1/5] dev stack 종료"
docker compose -f "${COMPOSE_FILE}" down

echo "[2/5] Postgres volume 확인"
POSTGRES_VOLUME=""
if POSTGRES_VOLUME=$(find_postgres_volume); then
  echo "삭제 대상 volume: ${POSTGRES_VOLUME}"
  docker volume rm "${POSTGRES_VOLUME}"
else
  echo "확인 필요: postgres_dev_data volume을 찾지 못했습니다. 이미 삭제된 상태로 보고 계속 진행합니다."
fi

echo "[3/5] Postgres 재기동"
docker compose -f "${COMPOSE_FILE}" up -d postgres

echo "[4/5] 나머지 dev 서비스 기동"
docker compose -f "${COMPOSE_FILE}" up -d temporal python-ai-worker control-plane temporal-worker

echo "[5/5] collation warning 확인"
if command -v rg >/dev/null 2>&1; then
  docker compose -f "${COMPOSE_FILE}" logs postgres | rg 'collation|mismatch' || true
else
  docker compose -f "${COMPOSE_FILE}" logs postgres | grep -E 'collation|mismatch' || true
fi

echo "완료: 필요하면 apps/control-plane/dev/smoke_semantic.sh 와 apps/control-plane/dev/smoke_cluster.sh 로 smoke를 다시 확인하세요."
