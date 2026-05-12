import type { PromptOperation } from "../types/prompt"

export const OPERATION_META: Record<
  PromptOperation,
  { label: string; groupLabel: string; badgeClass: string }
> = {
  prepare: {
    label: "prepare",
    groupLabel: "LLM 전처리",
    badgeClass: "bg-[#1e1e2e] text-[#e0e0ff] border-[#1e1e2e]",
  },
  prepare_batch: {
    label: "prepare_batch",
    groupLabel: "LLM 전처리 (배치)",
    badgeClass: "bg-muted text-muted-foreground border-border",
  },
  sentiment: {
    label: "sentiment",
    groupLabel: "감성 분석",
    badgeClass: "bg-blue-50 text-blue-600 border-blue-200",
  },
  sentiment_batch: {
    label: "sentiment_batch",
    groupLabel: "감성 분석 (배치)",
    badgeClass: "bg-amber-50 text-amber-600 border-amber-200",
  },
};

export const OPERATION_GROUP_ORDER: PromptOperation[] = [
  "prepare",
  "prepare_batch",
  "sentiment",
  "sentiment_batch",
];