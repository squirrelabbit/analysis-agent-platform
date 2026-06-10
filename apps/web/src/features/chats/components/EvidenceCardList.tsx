import type { ChatEvidence, ChatEvidenceChip } from "../models";
import {
  SENTIMENT_LABEL,
  SENTIMENT_BADGE_CLASS as SENTIMENT_CLASS,
  SENTIMENT_BADGE_FALLBACK,
} from "../models/theme";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";
import type { Taxonomy } from "@/features/taxonomy/models";

// 카드로 보여줄 최대 원문 수. 나머지는 상세 데이터에서 본다.
const MAX_EVIDENCE_CARDS = 5;

function chipLabel(chip: ChatEvidenceChip, taxonomy: Taxonomy | undefined): string {
  if (chip.key === ASPECT_FIELD) return aspectLabelOf(taxonomy, chip.value);
  return chip.value;
}

// 원문 샘플(sample_rows) — 표 대신 카드. 본문 + 감성 배지 + aspect/reason chip.
// 원문을 요약하거나 원인 추정하지 않는다 (그대로 노출).
export default function EvidenceCardList({ evidence }: { evidence: ChatEvidence }) {
  const { data: taxonomy } = useTaxonomy();
  const cards = evidence.items.slice(0, MAX_EVIDENCE_CARDS);
  const hidden = evidence.total - cards.length;

  return (
    <div className="mt-2 flex flex-col gap-2">
      {cards.map((item, idx) => {
        const sentClass = item.sentiment ? SENTIMENT_CLASS[item.sentiment] ?? SENTIMENT_BADGE_FALLBACK : "";
        return (
          <div key={item.id ?? idx} className="rounded-lg border border-zinc-200 bg-white p-3">
            <p className="text-sm text-zinc-800 whitespace-pre-wrap break-words">{item.text}</p>
            <div className="mt-2 flex flex-wrap items-center gap-1.5">
              {item.sentiment && (
                <span className={`rounded px-1.5 py-0.5 text-[11px] font-medium ${sentClass}`}>
                  {SENTIMENT_LABEL[item.sentiment] ?? item.sentiment}
                </span>
              )}
              {item.chips.map((chip) => (
                <span
                  key={chip.key}
                  className="rounded bg-violet-50 px-1.5 py-0.5 text-[11px] text-violet-700"
                >
                  {chipLabel(chip, taxonomy)}
                </span>
              ))}
            </div>
          </div>
        );
      })}
      {hidden > 0 && (
        <p className="text-xs text-zinc-400 px-1">외 {hidden}건 — 상세 데이터에서 확인</p>
      )}
    </div>
  );
}
