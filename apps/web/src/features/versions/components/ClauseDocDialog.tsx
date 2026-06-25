import { useMemo, useState, type ReactNode } from "react";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { aspectLabelOf } from "@/features/taxonomy/models";
import {
  SENTIMENT_FILTER_OPTIONS,
  SENTIMENT_LABELS,
  type Sentiment,
} from "@/features/versions/constants/sentiment";
import { cn } from "@/lib/utils";
import type { ClauseItem } from "../models/build";
import { FilterPills } from "./DataTable";

// 시안 「처리 현황 대시보드 (키워드 분석 탭 추가)」의 문서 다이얼로그.
// 문서 ID를 누르면 그 문서의 "원본 텍스트(추출된 절을 감성색으로 강조)" + "추출된 절"
// 목록을 보여준다. clause_label 결과에 절 단위 source/clause가 있으므로 현재 페이지에
// 로드된 절로 구성하고, 해당 문서의 절이 없으면 예시 데이터로 대체한다(요구사항).

interface DialogClause {
  clause: string;
  aspectLabel: string;
  sentiment: Sentiment;
}

// 절 데이터가 없을 때의 예시 문서(시안 genDocs 기반 하드코딩).
const FALLBACK_SRC =
  "올해 강릉문화재야행 다녀왔는데 드론쇼와 미디어아트 공연이 정말 환상적이었어요. 밤에 본 한옥마을 야경도 아름다웠고, 가족끼리 돌기에 딱 좋았습니다. 내년에도 꼭 다시 오고 싶어요!";
const FALLBACK_CLAUSES: DialogClause[] = [
  {
    clause: "드론쇼와 미디어아트 공연이 정말 환상적이었어요",
    aspectLabel: "공연/프로그램",
    sentiment: "positive",
  },
  {
    clause: "밤에 본 한옥마을 야경도 아름다웠고",
    aspectLabel: "분위기/경관",
    sentiment: "positive",
  },
  {
    clause: "내년에도 꼭 다시 오고 싶어요",
    aspectLabel: "재방문 의향",
    sentiment: "positive",
  },
];

// 감성별 강조 색 (시안 .dlg-src mark.pos/neg/neu).
const MARK_CLASS: Record<Sentiment, string> = {
  positive: "bg-emerald-50 [box-shadow:inset_0_-2px_0_#34d399]",
  negative: "bg-red-50 [box-shadow:inset_0_-2px_0_#f87171]",
  neutral: "bg-zinc-100 [box-shadow:inset_0_-2px_0_#a1a1aa]",
};

// 추출된 절 카드의 감성 배지 색 (시안 .dlg-clause .cverdict.pos/neg/neu).
const VERDICT_CLASS: Record<Sentiment, string> = {
  positive: "bg-emerald-50 text-emerald-700",
  negative: "bg-red-50 text-red-600",
  neutral: "bg-zinc-100 text-zinc-600",
};

// 절 행 왼쪽 감성 색상 바(긍/부/중립 좌측 보더).
const LEFT_BORDER: Record<Sentiment, string> = {
  positive: "border-l-emerald-400",
  negative: "border-l-red-400",
  neutral: "border-l-zinc-300",
};

function toSentiment(s: string): Sentiment {
  return s === "positive" || s === "negative" ? s : "neutral";
}

// 원본 텍스트에서 추출된 절을 좌→우로 탐욕적으로 찾아 감성색 mark로 감싼다.
// 절을 못 찾으면(원본에 substring으로 없으면) 그냥 평문으로 둔다.
function highlight(src: string, clauses: DialogClause[]): ReactNode[] {
  const nodes: ReactNode[] = [];
  let cursor = 0;
  let key = 0;
  while (cursor < src.length) {
    let best: { start: number; end: number; sent: Sentiment } | null = null;
    for (const c of clauses) {
      if (!c.clause) continue;
      const idx = src.indexOf(c.clause, cursor);
      if (idx === -1) continue;
      const better =
        !best ||
        idx < best.start ||
        (idx === best.start && c.clause.length > best.end - best.start);
      if (better) {
        best = { start: idx, end: idx + c.clause.length, sent: c.sentiment };
      }
    }
    if (!best) {
      nodes.push(src.slice(cursor));
      break;
    }
    if (best.start > cursor) nodes.push(src.slice(cursor, best.start));
    nodes.push(
      <mark
        key={key++}
        className={cn("rounded px-0.5 text-inherit", MARK_CLASS[best.sent])}
      >
        {src.slice(best.start, best.end)}
      </mark>,
    );
    cursor = best.end;
  }
  return nodes;
}

export default function ClauseDocDialog({
  docId,
  items,
  onClose,
}: {
  docId: string | null;
  items: ClauseItem[];
  onClose: () => void;
}) {
  const { data: taxonomy } = useTaxonomy();
  // 추출된 절 감성 필터 ("" | positive | neutral | negative).
  const [sentFilter, setSentFilter] = useState<string>("");

  const { src, clauses, isFallback } = useMemo(() => {
    if (!docId) {
      return { src: "", clauses: [] as DialogClause[], isFallback: false };
    }
    const docItems = items
      .filter((i) => i.docId === docId)
      .slice()
      .sort((a, b) => (a.sentenceIndex ?? 1e9) - (b.sentenceIndex ?? 1e9));

    // 현재 페이지에 이 문서의 절이 없으면 예시로 대체.
    if (docItems.length === 0) {
      return { src: FALLBACK_SRC, clauses: FALLBACK_CLAUSES, isFallback: true };
    }

    const cls: DialogClause[] = docItems.map((i) => ({
      clause: i.clause,
      aspectLabel: aspectLabelOf(taxonomy, i.aspect),
      sentiment: toSentiment(i.sentiment),
    }));

    // 원본 텍스트: 백엔드가 cleaned.parquet에서 LEFT JOIN한 cleaned_text(원본 문서
    // 정제 본문)를 우선 사용한다. 같은 docId 절은 동일 값이므로 첫 번째 비어있지 않은 값.
    // cleaned_text가 없으면(clean artifact 없음) 기존 fallback: source(원문) → 절 짜깁기.
    const cleanedText =
      docItems.map((i) => i.cleanedText).find((t) => !!t) ?? "";
    const sources = Array.from(
      new Set(docItems.map((i) => i.source).filter(Boolean)),
    );
    const sourcesText = sources.join(" ");
    const sourcesUsable =
      sources.length > 0 && cls.some((c) => sourcesText.includes(c.clause));
    const text = cleanedText
      ? cleanedText
      : sourcesUsable
        ? sourcesText
        : cls.map((c) => c.clause).join(" ");

    return { src: text, clauses: cls, isFallback: false };
  }, [docId, items, taxonomy]);

  // 감성 필터 적용한 표시 목록.
  const shown = sentFilter
    ? clauses.filter((c) => c.sentiment === sentFilter)
    : clauses;

  return (
    <Dialog
      open={!!docId}
      onOpenChange={(open) => {
        if (!open) {
          setSentFilter("");
          onClose();
        }
      }}
    >
      <DialogContent className="flex max-h-[86vh] flex-col gap-0 p-0 sm:max-w-160">
        <DialogHeader className="border-b border-zinc-100 px-5 py-4">
          <DialogTitle className="text-[15px]">
            원본 텍스트 · 추출된 절
          </DialogTitle>
          {docId && (
            <code className="mt-1.5 inline-block w-fit max-w-full truncate rounded-md bg-zinc-100 px-2 py-0.5 font-mono text-[11.5px] text-zinc-400">
              문서 ID {docId}
            </code>
          )}
        </DialogHeader>

        <div className="min-h-0 flex-1 overflow-y-auto px-5 py-5">
          {isFallback && (
            <p className="mb-3 rounded-lg bg-amber-50 px-3 py-2 text-[11px] text-amber-700">
              이 문서의 절 데이터가 없어 예시로 표시합니다.
            </p>
          )}

          <div className="mb-2 text-[11px] font-extrabold uppercase tracking-wider text-zinc-400">
            원본 텍스트
          </div>
          <div className="whitespace-pre-wrap rounded-xl border border-zinc-200 bg-zinc-50 px-4 py-3.5 text-sm leading-[1.75] text-zinc-800">
            {highlight(src, clauses)}
          </div>

          <div className="mb-2 mt-6 flex items-center justify-between gap-2">
            <div className="text-[11px] font-extrabold uppercase tracking-wider text-zinc-400">
              추출된 절 ({shown.length})
            </div>
            <div className="flex items-center gap-1">
              <FilterPills
                options={SENTIMENT_FILTER_OPTIONS}
                value={sentFilter}
                onChange={setSentFilter}
              />
            </div>
          </div>
          <div className="flex max-h-64 flex-col gap-2.25 overflow-y-auto pr-1">
            {shown.length === 0 ? (
              <p className="py-6 text-center text-xs text-zinc-400">
                해당 감성의 절이 없습니다.
              </p>
            ) : (
              shown.map((c, i) => (
                <div
                  key={i}
                  className={cn(
                    "flex items-center gap-3 rounded-[11px] border border-zinc-200 border-l-[3px] px-3.5 py-2.5",
                    LEFT_BORDER[c.sentiment],
                  )}
                >
                  <div
                    className="min-w-0 flex-1 truncate text-[13.5px] text-zinc-800"
                    title={c.clause}
                  >
                    {c.clause}
                  </div>
                  <span className="shrink-0 rounded-full bg-zinc-100 px-2.5 py-0.75 text-[11.5px] font-semibold text-zinc-600">
                    {c.aspectLabel}
                  </span>
                  <span
                    className={cn(
                      "shrink-0 rounded-full px-2.5 py-0.75 text-[11.5px] font-bold",
                      VERDICT_CLASS[c.sentiment],
                    )}
                  >
                    {SENTIMENT_LABELS[c.sentiment]}
                  </span>
                </div>
              ))
            )}
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
