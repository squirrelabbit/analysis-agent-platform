# DatasetService 책임 분해 제안 (조사 + 제안서)

> silverone 2026-06-04. 목적: `apps/control-plane/internal/service`의 `DatasetService`가
> 너무 많은 책임을 지고 있어 구조 파악 난이도가 높다. **이 문서는 조사·제안만**이며
> 대규모 코드 이동은 하지 않는다 (호출 관계 + risk를 먼저 문서화). 안전한 작은 이동만
> 별도 단계에서 진행한다.

## 1. 현재 상태

`DatasetService`는 단일 struct에 ~90개 메서드가 달려 있고, concern별로 파일이 나뉘어
있으나 **타입은 하나**다. 파일별 메서드 분포(대략):

| concern | 주요 파일 | public 메서드(예) |
|---|---|---|
| dataset core / CRUD | `datasets.go` (303) | CreateDataset / GetDataset / ListDatasets / DeleteDataset / UpdateDatasetMetadata / ActivateDatasetVersion / DeactivateDatasetVersion |
| dataset version | `dataset_versions.go` (422) | CreateDatasetVersion / UploadDatasetVersion / GetDatasetVersion / ListDatasetVersions / GetDatasetVersionDetail / DeleteDatasetVersion |
| dataset build | `dataset_build_jobs.go` (487) + `dataset_build_{clean,doc_genuineness,clause_label}.go` | CreateCleanJob / CreateDocGenuinenessJob / CreateClauseLabelJob / GetDatasetBuildJob / ListDatasetBuildJobs |
| analyze | `analyze.go` (392) + `analyze_response_projection.go` (213) | ExecuteAnalyze / ExecuteAnalyzeOnActiveVersion |
| analysis thread/run | `analysis_threads.go` (669) + `plan_reuse.go` (299) + `plan_step_display.go` (389) | CreateAnalysisThread / ListAnalysisThreads / GetAnalysisThread / DeleteAnalysisThread / GetAnalysisRun / AnalyzeDatasetAsNewThread / PostAnalysisThreadMessage |
| prompt | `dataset_prompts.go` (52, facade) + `service/datasetprompts/` | SaveProjectPrompt / ListProjectPrompts / GetProjectPromptDefaults / UpdateProjectPromptDefaults / ListProjectPromptHistory / RevertProjectPrompt / DiffProjectPromptVersions |
| 공유/기타 | `datasets.go` / `helpers.go` / `dataset_storage.go` / `dataset_artifacts.go` | buildClient / store 접근 / artifact 경로 derive / setter |

> prompt concern은 이미 `service/datasetprompts/` 서브패키지로 분리됐고 `dataset_prompts.go`는
> facade 위임만 한다 (2026-05-28 subpackage pilot). **분해 선례가 이미 있다.**

## 2. 호출 관계 (coupling)

핸들러(`internal/http`)는 모두 `datasetService.<Method>`를 직접 호출한다. concern 간
**내부 호출**과 **공유 의존**이 분해 난이도를 결정한다:

- `AnalyzeDatasetAsNewThread` (thread) → `ExecuteAnalyze` (analyze) + `s.store` thread/run 저장 +
  `buildConversationContext` / `tryReusePlan` (plan reuse).
- `ExecuteAnalyzeOnActiveVersion` (analyze) → active version resolve(`GetDataset`/version) → `ExecuteAnalyze`.
- `ExecuteAnalyze` (analyze) → `GetDatasetVersion` (version) + `resolveAnalyzeArtifactPaths` + `postPythonAITask` + projection.
- build job 들 → `GetDatasetVersion` (version) + `s.store` + `s.buildClient()`.

공유 의존 핵심: **`s.store`(Repository), dataset version lookup, artifact 경로 derive, `buildClient()`**.
즉 analyze / thread / build 모두 "dataset + version + artifact 경로"라는 core에 의존한다.

## 3. 제안 — 목표 타입 분리 (점진)

핸들러 호환을 위해 **`DatasetService`를 facade로 유지**하고, concern별 sub-service로 위임하는
방향을 권장한다 (prompt가 이미 쓰는 패턴과 동일). 한 번에 옮기지 않는다.

| 목표 타입 | 책임 | 의존 |
|---|---|---|
| `DatasetService` (core, 유지) | dataset/version CRUD, build orchestration, artifact 경로, store 보유 | store |
| `AnalyzeService` (신설 후보) | ExecuteAnalyze / ExecuteAnalyzeOnActiveVersion / artifact path resolve / worker 호출 / projection | core(version lookup, artifact path), python worker client |
| `AnalysisThreadService` (신설 후보) | thread/run CRUD + AnalyzeDatasetAsNewThread / PostAnalysisThreadMessage + plan reuse | AnalyzeService + store(thread/run) |
| `datasetprompts.Service` (이미 존재) | project prompt CRUD/history/diff | store |

권장 순서 (의존 역방향, 위험 낮은 것부터):
1. **analyze 타입/데이터 계약을 별도 파일로 분리** (logic과 contract 분리) — 무위험, 본 트랙 3단계에서 수행.
2. AnalyzeService 신설 + DatasetService facade 위임 (handler 시그니처 불변).
3. AnalysisThreadService 신설 (AnalyzeService에 의존).
4. core에서 build/version을 별도 타입으로 추가 분리(선택).

## 4. Risk

- **공유 store/helper**: sub-service 분리 시 `s.store`, version lookup, artifact 경로 helper를
  공유해야 한다. core 타입을 sub-service에 주입(생성자 인자)하는 방식이 안전.
- **내부 호출 체인**: thread → analyze → version 의 호출이 있어, 잘못 분리하면 순환 의존.
  AnalyzeService를 먼저, ThreadService가 그것을 의존하는 단방향으로 가야 한다.
- **테스트 결합**: 다수 테스트가 `&DatasetService{store: ...}`를 직접 구성하고 unexported
  필드/메서드를 호출한다(같은 package). sub-service로 옮기면 테스트도 함께 이동/수정 필요 →
  한 번에 큰 이동 금지, 패키지 내 점진 이동 권장.
- **죽은 코드 후보(별도 정리)**: `datasets.go`의 `deriveEmbeddingURI`/`deriveClusterURI`/
  `deriveSentimentURI`/`derivePrepareURI`/`deriveEmbeddingIndexSourceURI`는 ADR-018 β2로 제거된
  build 단계 잔재다. 일부는 호출 0건(`deriveClusterURI`, `deriveEmbeddingIndexSourceURI`),
  일부는 1건 남아 있다. **호출 경로 확인 후 별도 PR로 정리** (이번 범위 아님 — CLAUDE.md
  "호출 경로 확인 전 임의 삭제 금지").

## 5. 이번 트랙에서 실제 수행한 것

- 1단계(README 정합성)는 별도 MR(`docs/readme-refresh`)에서 완료.
- 3단계(안전한 작은 이동): **analyze 데이터 계약 타입을 `analyze_types.go`로 분리**
  (package 내 파일 이동, public API/동작 불변). AnalyzeService 신설은 위 순서 2로 후속.
