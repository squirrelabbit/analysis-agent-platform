# Prompt Templates

이 디렉터리는 Python AI worker가 사용하는 Markdown 기반 prompt template 저장소다.

원칙:

- 파일명에서 `.md`를 뺀 값이 prompt version이다.
- dataset profile의 `prepare_prompt_version`, `sentiment_prompt_version`은 이 파일명을 참조한다.
- 각 Markdown 파일은 선택적으로 front matter를 가진다.
  - `title`
  - `operation`
  - `status`
  - `summary`
- 예:
  - `dataset-prepare-anthropic-v1.md`
  - `dataset-prepare-anthropic-batch-v2.md`
  - `sentiment-anthropic-v2.md`
  - `sentiment-anthropic-batch-v1.md`
  - `execution-final-answer-v1.md`

placeholder:

- `{{raw_text}}`
- `{{rows_json}}`
- `{{text}}`
- `{{question}}`
- `{{scenario_json}}`
- `{{result_json}}`
- `{{evidence_json}}`

운영 메모:

- 기본 경로는 `config/prompts/`다.
- 필요하면 `PYTHON_AI_PROMPTS_DIR`로 다른 template 디렉터리를 지정할 수 있다.
- 기본 prompt version은 dataset profile이나 worker env에서 고른다.
- `prepare_prompt_version`, `sentiment_prompt_version`에 row 버전 이름을 넣어도 batch 실행 시 대응되는 `*-batch-*` prompt가 있으면 자동으로 그 버전을 사용한다.
- 최종 사용자 답변 레이어는 현재 worker env의 `ANTHROPIC_EXECUTION_FINAL_ANSWER_PROMPT_VERSION` 또는 기본값 `execution-final-answer-v1`을 사용한다.
- control plane의 `/dataset_profiles/validate`는 이 디렉터리를 읽어 prompt version 존재 여부와 front matter 기반 metadata를 확인한다.

기본값 변경 절차:

1. 새 prompt는 기존 파일을 덮어쓰지 말고 새 버전 파일로 추가한다.
2. front matter의 `summary`에 변경 의도를 짧게 적는다.
3. [CHANGELOG.md](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/prompts/CHANGELOG.md)에 버전 추가 배경을 남긴다.
4. 기본값을 바꾸려면 [dataset_profiles.json](/Users/silverone/00_workspace/01_work/05_TF_project/analysis-support-platform/config/dataset_profiles.json) 또는 관련 env를 함께 바꾼다.
5. 새 dataset version부터만 새 기본값을 쓰도록 하고, 기존 version은 재생성하지 않는 한 그대로 둔다.

확인 순서:

1. `curl -s http://127.0.0.1:18080/dataset_profiles/validate | python3 -m json.tool`
2. `PYTHONPATH=workers/python-ai/src python3 -m unittest discover -s workers/python-ai/tests -p 'test_*.py'`
