"""dataset_build 공통 문장 chunking (ADR-029).

문장 앵커 splitter + sentence-window chunk helper. doc_genuineness chunk aggregate와
clause_label verify가 공유한다 — **genuine_spans의 sentence_index가 두 skill에서
정합하려면 동일 splitter를 써야 한다**(핵심 제약).
"""
from __future__ import annotations

import re
from typing import Any

from .. import runtime as rt

_NON_ALNUM_KO = re.compile(r"[^가-힣A-Za-z0-9]")

_kiwi_singleton: Any = None


def _get_kiwi():
    global _kiwi_singleton
    if _kiwi_singleton is None:
        try:
            from kiwipiepy import Kiwi

            _kiwi_singleton = Kiwi()
        except Exception:  # noqa: BLE001 — 미설치/로드 실패 시 regex fallback
            _kiwi_singleton = False
    return _kiwi_singleton or None


def split_anchor_sentences(text: str) -> list[str]:
    """kiwipiepy 문장 분리 + 구두점-only 조각 drop(clean ". ." 잔재). kiwipiepy 미설치
    시 runtime regex fallback. doc_genuineness/clause_label verify 공통 splitter."""
    kiwi = _get_kiwi()
    if kiwi is not None:
        try:
            sents = [s.text.strip() for s in kiwi.split_into_sents(text)]
        except Exception:  # noqa: BLE001
            sents = rt._split_sentences(text, language="ko")[0]
    else:
        sents = rt._split_sentences(text, language="ko")[0]
    return [s for s in sents if s and _NON_ALNUM_KO.sub("", s)]


def build_sentence_chunks(
    sentences: list[str], *, max_sentences: int, max_chars: int, overlap: int
) -> list[tuple[int, list[str]]]:
    """문장 리스트를 (start0, sub) chunk로 나눈다. start0은 doc 전체 기준 0-based 오프셋.
    max_sentences/max_chars 중 먼저 도달하는 한도로 끊고, overlap만큼 다음 chunk가
    앞 chunk 끝과 겹친다(overlap=0이면 안 겹침). 단일 문장이 max_chars를 넘어도
    최소 1개는 넣는다(빈 chunk 방지)."""
    max_sentences = max(1, int(max_sentences))
    max_chars = max(1, int(max_chars))
    overlap = max(0, int(overlap))
    n = len(sentences)
    chunks: list[tuple[int, list[str]]] = []
    i = 0
    while i < n:
        end = i
        chars = 0
        while end < n and (end - i) < max_sentences and (end == i or chars + len(sentences[end]) <= max_chars):
            chars += len(sentences[end])
            end += 1
        chunks.append((i, sentences[i:end]))
        if end >= n:
            break
        nxt = end - overlap if overlap > 0 else end
        i = nxt if nxt > i else end  # 진행 보장(overlap >= chunk size 방어)
    return chunks
