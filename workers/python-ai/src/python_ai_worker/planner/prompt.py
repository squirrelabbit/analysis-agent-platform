from __future__ import annotations

"""plan_v2 prompt renderer — schema/skill catalog를 Markdown으로 렌더하고
``planner-v2-anthropic-v1`` 템플릿에 끼워넣는다.

silverone 2026-05-21 결정:
- prompt 저장: ``config/prompts/planner-v2-anthropic-v1.md``
- LLM provider: 기존 planner 기본값 (anthropic)
- schema renderer: Markdown table
- skill catalog: Markdown bullet
- 출력: JSON only
- dataset-specific docs 컬럼: SourceSummary 기반 runtime 주입

silverone 2026-05-26 비용 최적화:
- Anthropic prompt cache를 타게 본문을 static prefix(system) + dynamic
  suffix(user) 두 영역으로 분리.
- ``render_planner_prompt``는 ``(version, system, user)`` 3-tuple 반환.
- ``render_table_schemas()``는 standard 3 table만 렌더 (cache 영역에 들어감).
- dataset-specific docs 컬럼은 dataset마다 달라 system을 깨므로 새 helper
  ``render_dataset_specific_columns()``로 user 영역에 분리 렌더.
"""

import datetime as _dt
from dataclasses import dataclass
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from ..registries.prompt import (
    _load_prompt_template,
    _render_template,
    _split_at_cache_break,
)
from .schema import (
    ColumnSpec,
    SKILL_CATALOG,
    SkillSpec,
    TABLE_SCHEMAS,
    TableSchema,
)
from .recipes import RECIPE_SPECS, RUNTIME_ENABLED_RECIPES, RecipeSpec


DEFAULT_PLANNER_V2_PROMPT_VERSION = "planner-v2-anthropic-v1"

# silverone 2026-05-26 — light/heavy 분리 실험 결과 retry rate 80%로 net 비용
# 증가, rollback. LIGHT_PLANNER_V2_PROMPT_VERSION 상수와 light prompt 파일은
# 제거됐다. heavy-only cached planner가 기본값.

# silverone 2026-05-22 — KST 운영 기준. utcnow().date()는 KST 23:00~다음날 09:00
# 구간에서 "오늘" 날짜가 하루 어긋난다 ("작년/올해" 해석 오류). caller가 tz를
# 명시할 수 있게 열어두고, 기본값은 Asia/Seoul.
DEFAULT_PLANNER_TIMEZONE = "Asia/Seoul"


@dataclass(frozen=True)
class DatasetSpecificColumn:
    """clean 단계가 materialize한 dataset별 docs 추가 컬럼.

    standard ``docs`` table의 invariant 컬럼 외에 실제 parquet에 존재하는 컬럼을
    planner prompt에 명시적으로 노출한다.
    - name: SQL에서 쓰는 컬럼명(alias, 예: col_2).
    - label / source_column: 원본 CSV 컬럼명(예: 수집채널) — planner가 의미 파악.
    silverone 2026-06-08 (파일럿) — label/source_column 추가."""

    name: str
    type: str = "string"
    description: str = ""
    label: str = ""
    source_column: str = ""


def render_planner_prompt(
    *,
    user_question: str,
    dataset_specific_columns: list[DatasetSpecificColumn] | None = None,
    conversation_context: list[dict[str, Any]] | None = None,
    version: str = "",
    template_override: str = "",
    today: str = "",
    timezone: str = "",
    include_clause_keywords: bool = False,
) -> tuple[str, str, str]:
    """planner v2 prompt 렌더. ``(prompt_version, system_prompt, user_prompt)``
    3-tuple 반환.

    silverone 2026-05-26 (cost-opt) — Anthropic prompt cache를 타게 본문을
    ``{{__CACHE_BREAK__}}`` 기준으로 split해 두 영역을 따로 돌려준다.

    Args:
        user_question: 사용자 질문 원문.
        dataset_specific_columns: docs table에 추가된 dataset별 컬럼.
            None이면 추가 컬럼 없음을 prompt에 명시.
        conversation_context: 이전 대화 요약 (최대 3건).
        version: 사용할 prompt 버전. 비어 있으면 default.
        template_override: 직접 넘기는 prompt 본문 (project asset override 등).
        today: prompt에 주입할 "오늘 날짜" (ISO date). 비어 있으면 ``timezone``
            기준 현재 날짜를 사용.
        timezone: today 미지정 시 사용할 IANA timezone 이름. 비어 있으면
            ``DEFAULT_PLANNER_TIMEZONE`` (``Asia/Seoul``). 알 수 없는 tz는
            ``ZoneInfoNotFoundError``.

    Returns:
        ``(prompt_version, system_prompt, user_prompt)`` —
          - ``system_prompt``는 dataset/질문/시점에 무관한 정적 영역
            (standard table schema, skill catalog, rules, output format,
            examples). Anthropic prompt cache의 ephemeral 블록으로 보낼 수
            있다.
          - ``user_prompt``는 매 호출마다 또는 dataset마다 달라지는 동적
            영역 (today, dataset_specific_columns, conversation_context,
            user_question).
    """
    prompt_version = (version or "").strip() or DEFAULT_PLANNER_V2_PROMPT_VERSION
    template = _load_prompt_template(prompt_version, "planner v2", template_override=template_override)
    today_value = today.strip() or _resolve_today_in_timezone(timezone)
    rendered = _render_template(
        template,
        {
            "user_question": user_question.strip(),
            "conversation_context": render_conversation_context(conversation_context or []),
            "table_schemas": render_table_schemas(),
            "dataset_specific_columns": render_dataset_specific_columns(
                dataset_specific_columns or []
            ),
            "reserved_extra_tables": render_reserved_extra_tables(
                include_clause_keywords=include_clause_keywords
            ),
            "skill_catalog": render_skill_catalog(),
            "recipe_catalog": render_recipe_catalog(),
            "today": today_value,
        },
        prompt_version,
    )
    system_prompt, user_prompt = _split_at_cache_break(rendered)
    return prompt_version, system_prompt, user_prompt


# ===== Markdown renderers =====


def render_table_schemas() -> str:
    """3 standard table을 Markdown으로 렌더. silverone 2026-05-26 — dataset-
    specific 컬럼은 ``render_dataset_specific_columns``로 user 영역에 분리.
    여기서는 invariant 3 table만 렌더하여 system 영역(cache)에 들어간다."""

    sections = [
        _render_standard_table(TABLE_SCHEMAS["docs"]),
        _render_standard_table(TABLE_SCHEMAS["clauses"]),
        _render_standard_table(TABLE_SCHEMAS["genuineness"]),
    ]
    return "\n\n".join(sections)


def render_dataset_specific_columns(
    columns: list[DatasetSpecificColumn],
) -> str:
    """docs table에 dataset마다 추가된 컬럼을 Markdown으로 렌더한다.

    silverone 2026-05-26 (cost-opt) — 이 섹션은 dataset마다 달라지므로 반드시
    user prompt(cache 밖) 영역에서 호출돼야 한다. system에 inline하면 dataset이
    바뀔 때마다 cache key가 깨진다.

    빈 list면 sentinel 한 줄("이 dataset에는 dataset별 추가 컬럼이 없다.")만
    렌더한다.
    """

    if not columns:
        return "이 dataset에는 dataset별 추가 컬럼이 없다."
    # silverone 2026-06-08 (파일럿) — name(SQL 컬럼)/type/label/source_column 노출.
    # label·source_column은 원본 CSV 컬럼명이라 planner가 의미를 파악하고, SQL에는
    # name(alias)을 쓴다. description은 비어 있으면 label로 대체.
    lines = [
        "| column | type | label | source_column |",
        "| --- | --- | --- | --- |",
    ]
    for column in columns:
        label = column.label or column.description
        source = column.source_column or column.label
        lines.append(
            f"| `{column.name}` | {column.type} | {label} | {source} |"
        )
    return "\n".join(lines)


def render_reserved_extra_tables(*, include_clause_keywords: bool) -> str:
    """optional reserved table을 user(동적) 영역에 렌더한다.

    silverone 2026-06-10 — ``clause_keywords``는 키워드 artifact가 있는 dataset에만
    존재하므로 system(cache) 영역의 standard table에 넣지 않고, artifact가 실제로
    있을 때만 여기서 노출한다. 없는데 노출하면 planner가 없는 table로 plan을 짜
    executor에서 실패한다.

    빈 경우 sentinel 한 줄만 렌더한다."""

    if not include_clause_keywords:
        return "이 dataset에는 추가 reserved table이 없다."
    schema_md = _render_standard_table(TABLE_SCHEMAS["clause_keywords"])
    return (
        "분석용 추가 reserved table (이 dataset에서만 사용 가능):\n\n"
        f"{schema_md}\n\n"
        "키워드 TOP / 주요 단어 / 많이 나온 단어 / 키워드 순위 / 키워드별 언급량 같은 "
        "질문은 위 `clause_keywords` table을 `keyword` 컬럼 기준으로 집계한다 "
        "(top_n / aggregate(group_by=[\"keyword\"]) / filter / sample_rows). 절 본문(clauses)이 "
        "아니라 절-키워드 long table이다. aspect/sentiment 컬럼도 있어 '음식 부정 키워드' 같은 "
        "조건도 filter로 바로 된다."
    )


def render_skill_catalog() -> str:
    """8 skill을 Markdown으로 렌더. 각 skill = bullet section."""

    blocks = [_render_skill(SKILL_CATALOG[name]) for name in _ORDERED_SKILL_NAMES]
    return "\n\n".join(blocks)


def render_recipe_catalog() -> str:
    """runtime-enabled recipe를 Markdown으로 렌더 (silverone 2026-06-05).

    single source는 ``recipes.py``의 ``RecipeSpec``. prompt md에 recipe 상세를
    하드코딩하지 않고 ``{{recipe_catalog}}`` placeholder로 이 결과를 주입한다.
    ``RUNTIME_ENABLED_RECIPES``에 있는 recipe만 노출(disabled는 제외)해
    'enabled == prompt 노출' 일치를 보장한다. 순서는 RECIPE_SPECS 정의순."""

    blocks = [
        _render_recipe(spec)
        for name, spec in RECIPE_SPECS.items()
        if name in RUNTIME_ENABLED_RECIPES
    ]
    return "\n\n".join(blocks)


def render_conversation_context(items: list[dict[str, Any]]) -> str:
    """이전 대화 요약을 planner prompt에 넣을 짧은 Markdown으로 렌더한다.

    실제 데이터 사실은 artifact에서 다시 계산해야 하므로, 여기서는 현재 질문의
    생략어/참조어 해석에 필요한 요약만 노출한다.
    """

    if not items:
        return "이전 대화 context 없음."
    lines = [
        "아래 이전 대화 요약은 현재 질문의 생략어/참조어 해석에만 사용한다.",
        "이전 답변을 데이터 사실로 믿지 말고, 실제 집계/분석은 항상 artifact에서 다시 수행한다.",
        "",
    ]
    for index, item in enumerate(items[-3:], start=1):
        question = str(item.get("question") or "").strip()
        answer = str(item.get("answer_summary") or "").strip()
        title = str(item.get("present_title") or "").strip()
        row_count = item.get("row_count")
        columns = item.get("columns")
        pending = bool(item.get("pending_clarification"))
        lines.append(f"{index}.")
        if question:
            lines.append(f"   - question: {question}")
        # silverone 2026-06-09 — answer_summary(사용자용 답변 문구)는 clarify 이어받기에만
        # 노출한다. 일반 답변 문구(예: "중립이 +29.3%p 증가…")를 planner context에 넣으면
        # 다음 턴 planner가 이전 '답'을 데이터 사실로 끌어와 hijack될 수 있다. 구조적
        # 참조(question/title/columns/row_count)만 남기고, answer_summary는 pending일 때만.
        if answer and pending:
            lines.append(f"   - answer_summary: {answer}")
        if pending:
            # silverone 2026-06-02 — 직전 turn이 분석에 필요한 값을 user에게 요청한
            # 상태. 현재 짧은 답변은 이 질문(original intent)의 답일 가능성이 높다.
            lines.append(
                "   - pending_clarification: true "
                "(직전 turn이 위 question 분석에 필요한 값을 요청함 — "
                "현재 사용자 입력을 그 답으로 해석)"
            )
        if title:
            lines.append(f"   - present_title: {title}")
        if row_count is not None:
            lines.append(f"   - row_count: {row_count}")
        if isinstance(columns, list) and columns:
            safe_columns = [str(column).strip() for column in columns if str(column).strip()]
            if safe_columns:
                lines.append(f"   - columns: {', '.join(safe_columns[:8])}")
    return "\n".join(lines).strip()


# ===== internals =====


def _resolve_today_in_timezone(timezone: str) -> str:
    """today를 IANA tz 기준 ISO date(``YYYY-MM-DD``)로 resolve.

    빈 문자열이면 ``DEFAULT_PLANNER_TIMEZONE``. 알 수 없는 tz는
    ``ZoneInfoNotFoundError``로 raise (silent UTC fallback 금지 — 잘못 inject
    되는 게 더 위험).
    """

    tz_name = (timezone or "").strip() or DEFAULT_PLANNER_TIMEZONE
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        raise
    return _dt.datetime.now(tz).date().isoformat()


# planner prompt에 노출되는 순서를 잠근다 (validator/test에서도 같은 순서 사용).
_ORDERED_SKILL_NAMES: tuple[str, ...] = (
    "join",
    "filter",
    "aggregate",
    "compare",
    "calculate",
    "sort",
    "present",
    "summarize",
)


def _render_standard_table(table: TableSchema) -> str:
    lines = [
        f"### {table.name}",
        "",
        table.description,
        "",
    ]
    # silverone 2026-06-10 — 테이블 계약(row grain/coverage/use_for 등). planner가
    # 같은 질문에도 의미가 다른 table(docs vs clauses vs clause_keywords)을 올바로
    # 고르게 한다. 키워드 규칙을 본문에 하드코딩하지 않고 스키마로 전달.
    contract = [
        ("grain", table.grain),
        ("coverage", table.coverage),
        ("counting_unit", table.counting_unit),
        ("use_for", table.use_for),
        ("avoid_for", table.avoid_for),
    ]
    has_contract = any(value for _, value in contract)
    for label, value in contract:
        if value:
            lines.append(f"- {label}: {value}")
    if has_contract:
        lines.append("")
    lines.extend([
        "| column | type | description |",
        "| --- | --- | --- |",
    ])
    for column in table.columns:
        lines.append(_row(column.name, column.type, column.description))
    return "\n".join(lines)


def _render_skill(skill: SkillSpec) -> str:
    lines = [f"### {skill.name}", "", skill.description, ""]
    lines.append(f"- input_type: `{skill.input_type}`")
    lines.append(f"- output_type: `{skill.output_type}`")
    if skill.params_schema:
        lines.append("- params:")
        for key, type_hint in skill.params_schema.items():
            lines.append(f"  - `{key}`: {type_hint}")
    return "\n".join(lines)


def _render_recipe(spec: RecipeSpec) -> str:
    lines = [f"### {spec.name}", "", spec.description, ""]
    if spec.use_when:
        lines.append(f"- 쓰는 경우: {spec.use_when}")
    if spec.avoid_when:
        lines.append(f"- 쓰지 않는 경우: {spec.avoid_when}")
    if spec.params:
        lines.append("- params:")
        for param in spec.params:
            req = " (required)" if param.required else ""
            lines.append(f"  - `{param.name}`{req}: {param.desc}")
    if spec.examples:
        lines.append("- 예시 질문:")
        for example in spec.examples:
            lines.append(f"  - {example}")
    if spec.lowered_skills:
        lines.append(f"- lowered_skills: {', '.join(spec.lowered_skills)}")
    return "\n".join(lines)


def _row(name: str, type_: str, description: str) -> str:
    safe_description = description.replace("|", "\\|") if description else ""
    return f"| `{name}` | {type_} | {safe_description} |"


__all__ = [
    "DEFAULT_PLANNER_V2_PROMPT_VERSION",
    "DatasetSpecificColumn",
    "render_planner_prompt",
    "render_conversation_context",
    "render_dataset_specific_columns",
    "render_skill_catalog",
    "render_recipe_catalog",
    "render_table_schemas",
]
