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
| `DatasetBuildService` (신설 후보) | CreateCleanJob / CreateDocGenuinenessJob / CreateClauseLabelJob + build job 조회/dispatch | core(version lookup), worker build client, store(build_jobs) |
| `datasetprompts.Service` (이미 존재) | project prompt CRUD/history/diff | store |

### 분리 우선순위 (확정, silverone 2026-06-04)

선행으로 **analyze 데이터 계약 타입 분리**(`analyze_types.go`)는 완료(merged). 이후 순서:

1. **1순위 — AnalyzeService**: ExecuteAnalyze / ExecuteAnalyzeOnActiveVersion / artifact
   path resolve / worker 호출 / projection을 별도 타입으로. `DatasetService`는 facade로
   위임만(public API·handler 시그니처 불변). 의존이 단방향(아래가 위를 의존)이라 가장 먼저.
2. **2순위 — AnalysisThreadService**: thread/run CRUD + AnalyzeDatasetAsNewThread /
   PostAnalysisThreadMessage + plan reuse. **AnalyzeService에 의존**하므로 그 다음.
3. **3순위 — DatasetBuildService**: clean/doc_genuineness/clause_label job 생성·조회·dispatch.
   ⏸ **보류(prep-first)** — build 메서드가 store/version 외에 buildJobStarter(Temporal) +
   공유 core 헬퍼(buildClient / runWorkerTask / attachDatasetVersionArtifacts /
   datasetArtifactPathOrFallback / deriveCleanURI)에 의존하는데, 이 헬퍼들은 build 전용이
   아니라 analyze/version/storage/worker가 함께 쓰는 core 내부라 build service로 옮길 수 없다.
   지금 facade로 떼면 ~9 메서드 fat interface(약한 seam)가 되므로, 공유 헬퍼 경계를 먼저
   정리하는 prep MR 후 좁은 seam으로 분리한다.
4. **이후 — ADR-018 β2 legacy helper 정리**: §4의 죽은 코드 후보(derive*) 정리. ✅ 완료(MR !87).

각 단계는 작은 MR 1개로, facade 위임 + 테스트 동반, 동작/ public API 불변을 원칙으로 한다.

## 4. Risk

- **공유 store/helper**: sub-service 분리 시 `s.store`, version lookup, artifact 경로 helper를
  공유해야 한다. core 타입을 sub-service에 주입(생성자 인자)하는 방식이 안전.
- **내부 호출 체인**: thread → analyze → version 의 호출이 있어, 잘못 분리하면 순환 의존.
  AnalyzeService를 먼저, ThreadService가 그것을 의존하는 단방향으로 가야 한다.
- **테스트 결합**: 다수 테스트가 `&DatasetService{store: ...}`를 직접 구성하고 unexported
  필드/메서드를 호출한다(같은 package). sub-service로 옮기면 테스트도 함께 이동/수정 필요 →
  한 번에 큰 이동 금지, 패키지 내 점진 이동 권장.
- **죽은 코드 후보**: ✅ 정리 완료(MR !87). `datasets.go`의 derive* URI helper(embedding/
  cluster/sentiment/prepare, method+중복 func 8개)와 그것만 쓰던 `datasetSourceForUnstructured`를
  호출 경로 0건 확인 후 제거. 살아 있는 `deriveCleanURI`만 유지.

## 5. 진행 상태 (2026-06-04 갱신 — merge 반영)

- ✅ README 정합성 (MR !79)
- ✅ 조사 문서 + analyze 데이터 계약 타입 `analyze_types.go` 분리 (MR !83)
- ✅ **1순위 AnalyzeService facade 분리** (MR !85)
- ✅ **2순위 AnalysisThreadService facade 분리** (MR !89)
- ✅ ADR-018 β2 residue cleanup — derive* 죽은 코드 제거 (MR !87)
- ✅ Python 검증 명령 python3.11 정합 (MR !86) + stale 문서 정리 devtools/경로 (MR !88)
- ⏸ **3순위 DatasetBuildService — 보류(prep-first)**. 사유는 §3-3. 공유 core 헬퍼 경계를
  먼저 정리하지 않으면 fat interface(약한 seam)가 되므로 강행하지 않는다.

구조 분리는 **AnalyzeService / AnalysisThreadService까지로 일단락**한다. 이후 우선순위는
구조 리팩토링이 아니라 **운영 안정화 트랙**: worker 동시성/body 제한, 인증·노출 제어,
boot-time schema mutation 제거, artifact 검증, metrics.
