# Python AI Worker

이 디렉터리는 현재 런타임에서 Python이 맡는 AI worker와 비정형 분석 task 구현체다.

## 책임

- planner task
- dataset build task
  - `dataset_prepare`
  - `sentiment_label`
  - `embedding`
  - `dataset_cluster_build`
- unstructured `preprocess / aggregate / retrieve / summarize / presentation` skill 실행
- prompt template, rule config, embedding helper 관리
- `final_answer` 생성 task

## 코드 구조

| 위치 | 역할 |
| --- | --- |
| `src/python_ai_worker/main.py` | HTTP entrypoint |
| `src/python_ai_worker/task_router.py` | task name -> handler routing |
| `src/python_ai_worker/planner.py` | rule-based planner와 planner entrypoint |
| `src/python_ai_worker/prompt_registry.py` | prompt version -> Markdown template resolver |
| `src/python_ai_worker/skill_policy_registry.py` | skill policy version -> JSON policy resolver |
| `src/python_ai_worker/runtime` | payload, rule, artifact, embedding, LLM helper |
| `src/python_ai_worker/skills/dataset_build.py` | prepare, sentiment, embedding, cluster build |
| `src/python_ai_worker/skills/preprocess.py` | filter, dedup, sentence split, sample |
| `src/python_ai_worker/skills/aggregate.py` | keyword, noun, time/group count, taxonomy tagging |
| `src/python_ai_worker/skills/retrieve.py` | semantic search, cluster, cluster labeling |
| `src/python_ai_worker/skills/summarize.py` | issue summary, breakdown, trend, compare, evidence |
| `src/python_ai_worker/skills/presentation.py` | `final_answer` 후처리 task |
| `tests` | runtime helper, task, skill regression test |

참고:
- `preprocess.py`, `aggregate.py`, `retrieve.py`, `summarize.py`가 public skill entrypoint다.
- 실제 구현 본문은 같은 디렉터리의 private `*_impl.py` 파일로 나뉘어 있다.

## 현재 runtime 그룹

- dataset build
  - prepare / sentiment / embedding / cluster materialization
- preprocess
  - filter, dedup, sentence split, sample
- aggregate
  - keyword, noun, time/group count, taxonomy
- retrieve
  - semantic search, cluster, cluster labeling
- summarize
  - issue summary, breakdown, trend, compare, sentiment, evidence
- presentation
  - grounded `final_answer`

## prompt / rule / profile 연결

- global prompt template는 저장소 루트 [../../config/prompts](../../config/prompts) 아래 Markdown 파일로 관리한다.
- prompt version 이름은 파일명과 1:1로 대응한다.
- dataset profile 기본값은 [../../config/dataset_profiles.json](../../config/dataset_profiles.json) 에서 관리한다.
- project prompt version은 control plane의 `POST /projects/{project_id}/prompts`로 추가하고, project 기본 prompt 선택은 `PUT /projects/{project_id}/prompt_defaults`로 관리한다.
- 현재 project prompt override는 prepare/sentiment build payload의 inline template override로만 적용한다.
- skill policy 기본값은 [../../config/skill_policies](../../config/skill_policies) 아래 JSON 파일로 관리한다.
- rule config는 기본 상수 위에 `PYTHON_AI_RULE_CONFIG_PATH`, `PYTHON_AI_RULE_CONFIG_JSON`, request payload override를 순서대로 덮는다.

## 자주 쓰는 명령

```bash
PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.run_skill_case --validate
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.main --describe
```

로컬 임베딩 평가는 다음 명령을 사용한다.

```bash
PYTHONPATH=workers/python-ai/src python -m python_ai_worker.devtools.evaluate_embedding_model --model intfloat/multilingual-e5-small --format markdown
```
