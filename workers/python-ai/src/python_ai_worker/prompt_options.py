from __future__ import annotations

"""Task-folder prompt resolver (silverone 2026-06-02).

repo-root ``config/prompts/<task>/`` 아래 task별 prompt를 해석한다.

규칙:
  - task     = 폴더명 (``config/prompts/<task>/``)
  - version  = ``*.md`` 파일 stem
  - default  = ``index.yaml``의 ``default`` 값
  - label    = md 본문 첫 번째 H1(``# ...``). H1이 없으면 version fallback.

planner prompt는 옛 flat 구조(``config/prompts/planner-v2-anthropic-v1.md``)를
유지하므로 이 모듈은 task-folder prompt(doc_genuineness / clause_label) 전용이다.

API 노출 정책: :func:`list_prompt_options`는 prompt **본문/파일 경로를 반환하지
않는다** (version + label만). 본문은 dataset_build가 :func:`load_prompt_body`로만
읽는다.
"""

from pathlib import Path

from .config_paths import resolve_config_dir

PROMPTS_DIR_ENV = "PYTHON_AI_PROMPTS_DIR"

_INDEX_FILENAME = "index.yaml"
# version 후보에서 제외할 stem (md지만 prompt version이 아닌 보조 문서).
_EXCLUDE_STEMS = {"README", "CHANGELOG", "index"}


class PromptOptionsError(ValueError):
    """invalid task / index.yaml 오류 / 누락 default 등.

    ``ValueError`` 하위라 worker HTTP layer(main.py)가 400으로 매핑한다.
    """


def _prompts_root() -> Path:
    return resolve_config_dir(PROMPTS_DIR_ENV, __file__, "prompts")


def _task_dir(task: str) -> Path:
    name = (task or "").strip()
    # path traversal / 빈 값 방어. task는 단일 폴더명만 허용.
    if not name or "/" in name or "\\" in name or name.startswith("."):
        raise PromptOptionsError(f"invalid prompt task: {task!r}")
    task_dir = _prompts_root() / name
    if not task_dir.is_dir():
        raise PromptOptionsError(f"unknown prompt task: {name}")
    return task_dir


def _strip_front_matter(raw: str) -> str:
    """YAML front-matter(``---`` 블록) 제거 후 본문만 반환."""
    text = raw.lstrip()
    if not text.startswith("---"):
        return raw
    body = text[3:]
    end = body.find("\n---")
    if end < 0:
        return raw
    return body[end + 4 :].lstrip("\n")


def _first_h1_label(body: str, fallback: str) -> str:
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith("# "):
            label = stripped[2:].strip()
            return label or fallback
    return fallback


def _read_index_default(task_dir: Path) -> str:
    """``index.yaml``의 ``default`` version을 읽는다. 의존성 없는 단순 파서 —
    ``key: value`` 한 줄, ``#`` 뒤 주석 허용. 파일/키 누락은 PromptOptionsError."""
    index_path = task_dir / _INDEX_FILENAME
    if not index_path.is_file():
        raise PromptOptionsError(
            f"prompt task '{task_dir.name}' is missing {_INDEX_FILENAME}"
        )
    for raw_line in index_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].strip()
        if not line or ":" not in line:
            continue
        key, value = line.split(":", 1)
        if key.strip() == "default":
            default = value.strip().strip('"').strip("'")
            if default:
                return default
            break
    raise PromptOptionsError(
        f"prompt task '{task_dir.name}' {_INDEX_FILENAME} has no non-empty 'default'"
    )


def _version_stems(task_dir: Path) -> list[str]:
    return sorted(
        path.stem
        for path in task_dir.glob("*.md")
        if path.is_file() and path.stem not in _EXCLUDE_STEMS
    )


def resolve_prompt_path(task: str, version: str | None = None) -> Path:
    """task(+version) → ``config/prompts/<task>/<version>.md`` 경로.

    version 미지정 시 index.yaml default. 파일이 없으면 PromptOptionsError.
    """
    task_dir = _task_dir(task)
    ver = (version or "").strip() or _read_index_default(task_dir)
    path = task_dir / f"{ver}.md"
    if not path.is_file():
        raise PromptOptionsError(
            f"prompt task '{task_dir.name}' has no version '{ver}' ({ver}.md not found)"
        )
    return path


def load_prompt_body(task: str, version: str | None = None) -> tuple[str, str]:
    """(front-matter 제거된 본문, version stem). dataset_build 전용 — 파일 본문 로드."""
    path = resolve_prompt_path(task, version)
    body = _strip_front_matter(path.read_text(encoding="utf-8"))
    return body, path.stem


def list_prompt_options(task: str) -> dict:
    """task의 prompt 선택지. 본문/경로는 노출하지 않는다.

    반환: ``{"task": str, "default": str, "versions": [{"version", "label"}]}``.
    index.yaml default가 실제 md로 존재하지 않으면 PromptOptionsError.
    """
    task_dir = _task_dir(task)
    default = _read_index_default(task_dir)
    versions = _version_stems(task_dir)
    if default not in versions:
        raise PromptOptionsError(
            f"prompt task '{task_dir.name}' index default '{default}' "
            f"has no matching md file (available versions: {versions})"
        )
    options: list[dict[str, str]] = []
    for stem in versions:
        body = _strip_front_matter((task_dir / f"{stem}.md").read_text(encoding="utf-8"))
        options.append({"version": stem, "label": _first_h1_label(body, stem)})
    return {"task": task_dir.name, "default": default, "versions": options}


__all__ = [
    "PROMPTS_DIR_ENV",
    "PromptOptionsError",
    "list_prompt_options",
    "load_prompt_body",
    "resolve_prompt_path",
]
