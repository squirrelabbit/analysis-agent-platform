# Quality eval set v1 (datasets only)

> **2026-06-16 정리**: quality eval의 case/runner 트랙은 폐기됐다. 검증
> 대상이던 skill(`issue_evidence_summary` / `execution_final_answer`)은
> δ-1~δ-4·ADR-018로 삭제됐고, runner(`python_ai_worker.devtools.run_eval_case`)도
> δ-1 devtools 제거로 사라졌다. 따라서 `cases/`는 제거됐다.
>
> 살아남은 것은 `datasets/`의 sample CSV뿐이다 — smoke 스크립트
> (`scripts/smoke_doc_genuineness_verify.sh`, `scripts/smoke_preprocess_pipeline.sh`)가
> 입력 데이터로 참조하므로 유지한다.

## Dataset

| 파일 | 도메인 | rows | 출처 |
| --- | --- | --- | --- |
| `festival_sample_50.csv` | 강릉 문화재 야행 SNS 수집 (네이버 블로그·카페) | 50 | `data/festival.csv`(2121 rows) stratified sample |
| `festival_sample_empty.csv` | 동일 (header-only) | 0 | negative test fixture |

익명화 처리:
- `작성자` → SHA256 8자 prefix (`anon-XXXXXXXX`)
- `URL` → `https://example.invalid/{cafe|blog|post}/<hash>`
- `본문` → "존재하지 않는 이미지입니다." 노이즈 제거 외에는 원본 유지

## 관련 트랙

- `docs/eval/skill_regression_v1.yaml` — planner correctness regression (별도 트랙).
