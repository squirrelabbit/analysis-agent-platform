from __future__ import annotations

"""prompt template loader — δ-4 (2026-05-21) 이후 v2 layer가 쓰는 4 helper만
유지한다.

- :func:`_load_prompt_template` — version → markdown body (front-matter
  strip 포함). planner가 직접 호출.
- :func:`_render_template` — ``{{key}}`` placeholder 치환 + unresolved
  검사.
- :func:`_strip_cache_break` / :func:`_split_at_cache_break` — Anthropic
  prompt cache marker(``{{__CACHE_BREAK__}}``) 처리.
- :func:`available_prompt_versions` / :func:`prompt_catalog` — 운영 도구
  + test_prompt_registry용 read-only API.

옛 helper(``render_execution_final_answer_prompt`` /
``render_issue_summary_view_prompt`` / ``render_issue_evidence_summary_prompt`` /
``render_planner_prompt`` 등)는 모두 제거됐다.

이 모듈은 **flat 구조 prompt**(planner-v2-anthropic-v1.md 등 config/prompts
바로 아래 ``*.md``)만 다룬다. dataset_build의 doc_genuineness / clause_label은
silverone 2026-06-02부터 task-folder 구조(``config/prompts/<task>/<version>.md``
+ index.yaml)로 이관됐고 ``python_ai_worker.prompt_options``가 해석한다.
"""

import re
from pathlib import Path

from ..config_paths import resolve_config_dir

PROMPTS_DIR_ENV = "PYTHON_AI_PROMPTS_DIR"

_PROMPT_DIR_EXCLUDE = {"README", "CHANGELOG"}
_CACHE_BREAK_MARKER = "{{__CACHE_BREAK__}}"


def _prompt_templates_dir() -> Path:
    return resolve_config_dir(PROMPTS_DIR_ENV, __file__, "prompts")


def _available_prompt_versions() -> list[str]:
    templates_dir = _prompt_templates_dir()
    if not templates_dir.exists():
        return []
    return sorted(
        path.stem
        for path in templates_dir.glob("*.md")
        if path.is_file() and path.stem not in _PROMPT_DIR_EXCLUDE
    )


def available_prompt_versions() -> list[str]:
    return _available_prompt_versions()


def _parse_front_matter(raw_text: str) -> tuple[dict[str, str], str]:
    content = raw_text.strip()
    if not content.startswith("---\n"):
        return {}, content
    lines = content.splitlines()
    metadata: dict[str, str] = {}
    closing_index = -1
    for index, line in enumerate(lines[1:], start=1):
        if line.strip() == "---":
            closing_index = index
            break
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        metadata[key.strip()] = value.strip()
    if closing_index < 0:
        return {}, content
    body = "\n".join(lines[closing_index + 1 :]).strip()
    return metadata, body


def _infer_prompt_operation(version: str) -> str:
    """Best-effort operation name inference from prompt filename.

    Used by :func:`prompt_catalog` to populate a fallback when the
    front-matter omits ``operation:``. v2 layer prompts use explicit
    front-matter so this is just defensive.
    """
    normalized = version.strip()
    if normalized.startswith("planner-v2"):
        return "planner"
    if normalized.startswith("dataset-doc-genuineness"):
        return "doc_genuineness"
    if normalized.startswith("dataset-clause-label"):
        return "clause_label"
    return "custom"


def prompt_catalog() -> list[dict[str, object]]:
    catalog: list[dict[str, object]] = []
    for version in _available_prompt_versions():
        template_path = _prompt_templates_dir() / f"{version}.md"
        metadata, _ = _parse_front_matter(template_path.read_text(encoding="utf-8"))
        title = str(metadata.get("title") or version).strip()
        operation = str(metadata.get("operation") or _infer_prompt_operation(version)).strip()
        status = str(metadata.get("status") or "active").strip()
        summary = str(metadata.get("summary") or "").strip()
        catalog.append(
            {
                "version": version,
                "title": title,
                "operation": operation,
                "status": status,
                "summary": summary,
            }
        )
    return catalog


def _load_prompt_template(version: str, kind: str, template_override: str = "") -> str:
    override = str(template_override or "").strip()
    if override:
        _, body = _parse_front_matter(override)
        if body.strip():
            return body.strip()
        raise ValueError(f"{kind} prompt template override is empty")
    normalized_version = version.strip()
    template_path = _prompt_templates_dir() / f"{normalized_version}.md"
    if not template_path.is_file():
        available = ", ".join(_available_prompt_versions())
        raise ValueError(f"unsupported {kind} prompt version: {normalized_version} (available: {available})")
    _, body = _parse_front_matter(template_path.read_text(encoding="utf-8"))
    return body.strip()


def _render_template(template: str, replacements: dict[str, str], version: str) -> str:
    rendered = template
    for key, value in replacements.items():
        rendered = rendered.replace(f"{{{{{key}}}}}", value)
    unresolved = sorted(
        set(re.findall(r"{{\s*([a-zA-Z0-9_]+)\s*}}", rendered)) - {"__CACHE_BREAK__"}
    )
    if unresolved:
        raise ValueError(f"prompt template {version} has unresolved placeholders: {', '.join(unresolved)}")
    return rendered


def _strip_cache_break(rendered: str) -> str:
    if _CACHE_BREAK_MARKER not in rendered:
        return rendered
    return rendered.replace(_CACHE_BREAK_MARKER, "").strip()


def _split_at_cache_break(rendered: str) -> tuple[str, str]:
    if _CACHE_BREAK_MARKER not in rendered:
        return "", rendered.strip()
    system_part, _, user_part = rendered.partition(_CACHE_BREAK_MARKER)
    return system_part.strip(), user_part.strip()


__all__ = [
    "PROMPTS_DIR_ENV",
    "available_prompt_versions",
    "prompt_catalog",
    "_load_prompt_template",
    "_render_template",
    "_split_at_cache_break",
    "_strip_cache_break",
]
