#!/usr/bin/env python3.11
"""Compare product Kiwi keyword extraction with the analysis-team Okt prototype.

This is an offline harness only. It intentionally does not add konlpy/Okt to the
product worker dependency graph. If konlpy is absent, the script still prints
Kiwi output and clear setup guidance.
"""

from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
PYTHON_AI_SRC = REPO_ROOT / "workers" / "python-ai" / "src"
if str(PYTHON_AI_SRC) not in sys.path:
    sys.path.insert(0, str(PYTHON_AI_SRC))

from python_ai_worker.dataset_build.keyword_extractor import (  # noqa: E402
    FESTIVAL_STOPWORDS,
    KiwiKeywordExtractor,
)


DEFAULT_SAMPLES = [
    "푸드트럭 가격이 생각보다 비싸고 대기줄이 길었어요",
    "드론쇼는 예뻤지만 교통 통제가 부족해서 이동이 불편했습니다",
    "강릉 단오 끝나고 문화재야행 행사도 있어서 볼거리가 많았습니다",
    "공연 프로그램은 좋았는데 화장실 위치 안내가 부족했습니다",
    "아이와 체험 부스를 방문했는데 운영 staff가 친절했습니다",
]


class OktKeywordExtractor:
    """Analysis-team prototype behavior, kept out of product imports."""

    version = "okt-noun-prototype"

    def __init__(self, *, min_len: int = 2, stopwords: set[str] | frozenset[str] = FESTIVAL_STOPWORDS) -> None:
        try:
            from konlpy.tag import Okt
        except Exception as exc:  # pragma: no cover - depends on local optional env
            raise RuntimeError(
                "konlpy/Okt is not installed. Install it only in a local comparison env "
                "(not the product worker image), then rerun this script."
            ) from exc
        self._okt = Okt()
        self._min_len = max(1, int(min_len))
        self._stopwords = {str(w).strip() for w in stopwords if str(w).strip()}

    def extract(self, text: str) -> list[str]:
        if not text or not str(text).strip():
            return []
        seen: set[str] = set()
        keywords: list[str] = []
        for raw in self._okt.nouns(str(text)):
            token = str(raw or "").strip()
            if not token or token in seen:
                continue
            if len(token) < self._min_len:
                continue
            if token in self._stopwords:
                continue
            if re.fullmatch(r"\d+", token):
                continue
            seen.add(token)
            keywords.append(token)
        return keywords


def _read_jsonl(path: Path, field: str) -> list[str]:
    rows: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            item = json.loads(line)
            if isinstance(item, dict):
                text = str(item.get(field) or "").strip()
                if text:
                    rows.append(text)
    return rows


def _read_csv(path: Path, field: str) -> list[str]:
    rows: list[str] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for item in reader:
            text = str(item.get(field) or "").strip()
            if text:
                rows.append(text)
    return rows


def _read_text(path: Path) -> list[str]:
    return [line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def read_samples(path: Path | None, *, field: str, inline_texts: list[str], limit: int) -> list[str]:
    samples: list[str] = []
    samples.extend([text.strip() for text in inline_texts if text.strip()])
    if path is not None:
        suffix = path.suffix.lower()
        if suffix == ".jsonl":
            samples.extend(_read_jsonl(path, field))
        elif suffix == ".csv":
            samples.extend(_read_csv(path, field))
        else:
            samples.extend(_read_text(path))
    if not samples:
        samples = list(DEFAULT_SAMPLES)
    return samples[: max(1, limit)]


def _module_available(name: str) -> bool:
    return importlib.util.find_spec(name) is not None


def compare(
    samples: list[str],
    *,
    min_len: int,
    require_kiwi: bool,
    require_okt: bool,
) -> tuple[list[dict[str, Any]], str | None, str | None]:
    kiwi_error: str | None = None
    if not _module_available("kiwipiepy"):
        kiwi_error = (
            "kiwipiepy is not installed in this local environment. "
            "KiwiKeywordExtractor will use runtime regex fallback; install worker deps "
            "for a valid Kiwi-vs-Okt quality comparison."
        )
        if require_kiwi:
            raise RuntimeError(kiwi_error)
    kiwi = KiwiKeywordExtractor(min_len=min_len)
    okt_error: str | None = None
    okt: OktKeywordExtractor | None = None
    try:
        okt = OktKeywordExtractor(min_len=min_len)
    except RuntimeError as exc:
        okt_error = str(exc)
        if require_okt:
            raise

    rows: list[dict[str, Any]] = []
    for idx, text in enumerate(samples, start=1):
        kiwi_keywords = kiwi.extract(text)
        okt_keywords = okt.extract(text) if okt is not None else None
        rows.append(
            {
                "index": idx,
                "clause": text,
                "kiwi": kiwi_keywords,
                "okt": okt_keywords,
                "only_kiwi": sorted(set(kiwi_keywords) - set(okt_keywords or [])),
                "only_okt": sorted(set(okt_keywords or []) - set(kiwi_keywords)),
            }
        )
    return rows, kiwi_error, okt_error


def _join(items: list[str] | None) -> str:
    if items is None:
        return "(Okt unavailable)"
    return ", ".join(items) if items else "-"


def render_markdown(rows: list[dict[str, Any]], *, kiwi_error: str | None, okt_error: str | None) -> str:
    lines = [
        "# Kiwi vs Okt Keyword Extraction",
        "",
        "- Product default: Kiwi (`KiwiKeywordExtractor`)",
        "- Okt: offline comparison only; not a product worker dependency",
        "- Stopwords: shared festival stopwords from analysis-team prototype",
        "",
    ]
    if kiwi_error:
        lines.extend(
            [
                "## Kiwi fallback warning",
                "",
                f"`{kiwi_error}`",
                "",
            ]
        )
    if okt_error:
        lines.extend(
            [
                "## Okt unavailable",
                "",
                f"`{okt_error}`",
                "",
            ]
        )
    lines.extend(
        [
            "| # | clause | Kiwi | Okt | only Kiwi | only Okt |",
            "|---:|---|---|---|---|---|",
        ]
    )
    for row in rows:
        clause = str(row["clause"]).replace("|", "\\|")
        lines.append(
            "| {index} | {clause} | {kiwi} | {okt} | {only_kiwi} | {only_okt} |".format(
                index=row["index"],
                clause=clause,
                kiwi=_join(row["kiwi"]),
                okt=_join(row["okt"]),
                only_kiwi=_join(row["only_kiwi"]),
                only_okt=_join(row["only_okt"]),
            )
        )
    lines.append("")
    return "\n".join(lines)


def render_jsonl(rows: list[dict[str, Any]]) -> str:
    return "\n".join(json.dumps(row, ensure_ascii=False) for row in rows) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Compare Kiwi and Okt keyword extraction on clause samples.")
    parser.add_argument("--input", type=Path, help="Input .jsonl/.csv/text file. JSONL/CSV use --field.")
    parser.add_argument("--field", default="clause", help="Text field for JSONL/CSV input. Default: clause.")
    parser.add_argument("--text", action="append", default=[], help="Inline clause text. Can be repeated.")
    parser.add_argument("--limit", type=int, default=50, help="Max sample rows. Default: 50.")
    parser.add_argument("--min-len", type=int, default=2, help="Minimum keyword length. Default: 2.")
    parser.add_argument("--format", choices=("markdown", "jsonl"), default="markdown")
    parser.add_argument("--output", type=Path, help="Write report to file instead of stdout.")
    parser.add_argument("--require-kiwi", action="store_true", help="Fail if kiwipiepy is unavailable.")
    parser.add_argument("--require-okt", action="store_true", help="Fail if konlpy/Okt is unavailable.")
    args = parser.parse_args(argv)

    samples = read_samples(args.input, field=args.field, inline_texts=args.text, limit=args.limit)
    try:
        rows, kiwi_error, okt_error = compare(
            samples,
            min_len=args.min_len,
            require_kiwi=args.require_kiwi,
            require_okt=args.require_okt,
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    report = (
        render_jsonl(rows)
        if args.format == "jsonl"
        else render_markdown(rows, kiwi_error=kiwi_error, okt_error=okt_error)
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(report, encoding="utf-8")
    else:
        print(report, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
