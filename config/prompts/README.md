# Prompt Templates

이 디렉터리는 Python AI worker가 사용하는 Markdown 기반 prompt template 저장소다.

원칙:

- 파일명에서 `.md`를 뺀 값이 prompt version이다.
- dataset profile의 `prepare_prompt_version`, `sentiment_prompt_version`은 이 파일명을 참조한다.
- 예:
  - `dataset-prepare-anthropic-v1.md`
  - `dataset-prepare-anthropic-batch-v2.md`
  - `sentiment-anthropic-v2.md`

placeholder:

- `{{raw_text}}`
- `{{rows_json}}`
- `{{text}}`

운영 메모:

- 기본 경로는 `config/prompts/`다.
- 필요하면 `PYTHON_AI_PROMPTS_DIR`로 다른 template 디렉터리를 지정할 수 있다.
