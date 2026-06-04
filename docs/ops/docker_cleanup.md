# Docker 이미지/빌드 정리 런북 (operator-run)

> silverone 2026-06-04. dev/staging 박스의 Docker 디스크 사용을 줄이는 **수동** 절차.
> ⚠️ **자동 실행 금지** (compose up / CI / cron에 넣지 않는다). prune은 사용 중이지
> 않은 이미지·캐시를 **영구 삭제**하므로 operator가 상태를 확인하고 직접 실행한다.

## 0. 배경 — 이번 정리로 줄어든 것

- **python-ai-worker**: Dockerfile에서 `scikit-learn` 제거(코드 사용처 0건, ADR-018 β2
  cluster skill 잔재). 이미지 **약 1.04GB → 771MB** (~270MB 감소).
- **control-plane / temporal-worker**: 같은 Dockerfile에서 `server`/`temporal-worker` 두
  바이너리를 빌드한 동일 산출물 → compose에서 **같은 image tag(`analysis-support-platform-control-plane:dev`)를 공유**. 차이는 command뿐. 이전엔 동일 내용 이미지가 2개(각 ~360MB)였다.

→ 위 변경 후, 옛 태그(`...-control-plane:latest`, `...-temporal-worker:latest`)와 옛
python-ai-worker 레이어가 **미사용/dangling**으로 남는다. 아래 절차로 회수한다.

## 1. 현재 사용량 확인 (삭제 전 항상)

```bash
docker system df              # 이미지/컨테이너/볼륨/빌드캐시 총량 요약
docker images                 # 태그별 이미지 크기
docker images -f dangling=true   # 태그 없는(dangling) 이미지만
```

지우기 전에 **실행 중 컨테이너가 쓰는 이미지**를 확인한다:

```bash
docker compose -f compose.dev.yml ps           # 어떤 서비스가 떠 있나
docker ps --format '{{.Image}}\t{{.Names}}'    # 사용 중 이미지
```

## 2. 안전한 단계부터 회수

```bash
# (a) dangling 이미지만 삭제 — 태그 없는 옛 레이어. 가장 안전.
docker image prune

# (b) 빌드 캐시 삭제 — 다음 빌드가 느려질 뿐, 산출물엔 영향 없음.
docker builder prune

# (c) 이번 변경으로 대체된 옛 태그를 명시적으로 제거 (해당 이미지를 쓰는 컨테이너가
#     없을 때만). 공유 tag(:dev)로 전환했으므로 아래 :latest 2개는 더는 안 쓰인다.
docker rmi analysis-support-platform-control-plane:latest \
           analysis-support-platform-temporal-worker:latest
```

## 3. 더 공격적인 회수 (주의)

```bash
# 사용 중이지 않은 *모든* 이미지 삭제 — base 이미지까지 지워져 다음 빌드 때 재pull/재빌드.
# 시간 여유가 있고 디스크가 급할 때만.
docker image prune -a

# 컨테이너/네트워크/이미지/빌드캐시 일괄 (볼륨 제외) — 영향 범위가 넓으니 신중히.
docker system prune
```

⚠️ `docker volume prune` / `docker system prune --volumes` 는 **postgres 데이터 볼륨
(`postgres_dev_data`)을 지울 수 있다.** dataset/분석 이력이 날아가므로 이번 런북 범위에서
제외한다. 필요 시 볼륨을 개별 확인(`docker volume ls`) 후 신중히.

## 4. 재빌드 (정리 후 스택 복구)

```bash
docker compose -f compose.dev.yml up -d --build
# staging: docker compose -f compose.dev.yml -f compose.staging.yml up -d --build
```

`up --build`는 control-plane이 공유 image(`:dev`)를 빌드하고 temporal-worker가 그 image를
재사용한다(temporal-worker는 build 정의 없이 image+command만 가진다).

## 5. 검증 (정리·재빌드 후)

```bash
docker compose -f compose.dev.yml ps    # control-plane / temporal-worker 가 같은 IMAGE인지
curl -s http://127.0.0.1:18080/health   # control-plane 응답
curl -s http://127.0.0.1:18090/readyz   # python-ai-worker readiness
./scripts/smoke_worker_limits.sh        # worker 제한 점검 (선택)
```
