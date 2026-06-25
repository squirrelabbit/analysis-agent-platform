"""dataset_build 프롬프트의 행사별 추가 슬롯(extra_instructions / extra_examples).

clause_label / doc_genuineness 가 **각자 자기 task의 metadata 서브객체**에서 읽는다.
task별로 분리하는 이유: 두 task는 출력 스키마가 달라(문서 3-tier vs 문장 배열)
예시를 공용으로 두면 한 task 프롬프트에 다른 task 예시가 섞여 스키마가 오염된다.

값이 비면 ``""`` 를 돌려줘서 프롬프트의 ``{{#if extra_*}}`` 블록이 통째 생략되게 한다.
슬롯은 base 프롬프트 끝에 append-only로 붙는다(본문 치환·삭제 없음).
"""

from __future__ import annotations

from typing import Any


def render_extra_examples(value: Any) -> str:
    """extra_examples 를 프롬프트에 붙일 문자열로 직렬화.

    문자열이면 그대로(trim), 리스트면 ``예시 N:`` 블록으로 결합. 비면 ``""``.
    """
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        blocks: list[str] = []
        idx = 0
        for item in value:
            text = str(item).strip()
            if not text:
                continue
            idx += 1
            blocks.append(f"예시 {idx}:\n{text}")
        return "\n\n".join(blocks)
    return ""


def extract_extra_slot(raw: Any) -> dict[str, str]:
    """task별 metadata 서브객체(raw)에서 extra_instructions / extra_examples 추출.

    raw 가 dict 가 아니거나 키가 없으면 빈 문자열. 항상 두 키를 가진 dict 를 반환해
    호출부가 분기 없이 prompt config 에 병합할 수 있게 한다.
    """
    if not isinstance(raw, dict):
        return {"extra_instructions": "", "extra_examples": ""}
    instructions = str(raw.get("extra_instructions") or "").strip()
    examples = render_extra_examples(raw.get("extra_examples"))
    return {"extra_instructions": instructions, "extra_examples": examples}
