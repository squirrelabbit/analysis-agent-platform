import type { Prompt } from "@/features/prompt/types/prompt";

// ── 프롬프트 목록 ──────────────────────────────────────────
export const MOCK_PROMPTS: Prompt[] = [
  {
    id: "1",
    version: "v1",
    contentHash: "",
    createdAt: "",
    operation: "prepare_batch",
    status: "active",
    summary: " prepare batch 최초 버전. row 순서 유지와 독립 처리를 강조한다.",
    title: "Dataset prepare batch v1",
    updatedAt: "",
    content: `
    ## Task

    You are preparing raw VOC or issue text for deterministic downstream analysis.

    - Process each row independently and preserve ordering.
    - Keep the original meaning.
    - Remove only obvious noise, duplicated punctuation, and boilerplate.
    - Do not summarize beyond a short normalization.
    - Do not invent facts.
    - Choose disposition 'keep', 'review', or 'drop' for each row.
    `,
  },
  {
    id: "2",
    version: "v2",
    contentHash: "",
    createdAt: "",
    operation: "prepare_batch",
    status: "active",
    summary: "이슈 세부정보 보존과 과도한 요약 방지를 강화한 prepare batch 프롬프트",
    title: "Dataset prepare batch v2",
    updatedAt: "",
    content: `
    ## Task

    You are preparing raw VOC or issue text for deterministic downstream analysis.

    - Process each row independently, preserve ordering, and preserve issue-specific details.
    - Normalize only obvious noise, duplicated punctuation, spacing, and boilerplate.
    - Do not summarize, merge rows, or infer missing context.
    - Choose exactly one disposition: 'keep', 'review', or 'drop' for each row.

    ## Rows

    {{rows_json}}
    `,
  },
  {
    id: "3",
    version: "v1",
    contentHash: "",
    createdAt: "",
    operation: "prepare",
    status: "active",
    summary: "초기 row 단위 prepare 프롬프트. 기본 정제와 keep/review/drop 판정을 수행한다.",
    title: "Dataset prepare row v1",
    updatedAt: "",
    content: `
    ## Task

    You are preparing raw VOC or issue text for deterministic downstream analysis.

    - Keep the original meaning.
    - Remove only obvious noise, duplicated punctuation, and boilerplate.
    - Do not summarize beyond a short normalization.
    - Do not invent facts.
    - Choose disposition 'keep', 'review', or 'drop'.
    - Use 'drop' only for empty, unreadable noise, or clear non-content rows.
    - Use 'review' when the text is partially readable but low quality or mixed.

    ## Raw Text

    {{raw_text}}
    `,
  },
  {
    id: "4",
    version: "v1",
    contentHash: "",
    createdAt: "",
    operation: "sentiment_batch",
    status: "active",
    summary: "감성 라벨링 batch 최초 버전. row 순서 유지와 JSON 배열 응답을 요구한다.",
    title: "Sentiment batch v1",
    updatedAt: "",
    content: `
    ## Task

    You are labeling sentiment for customer feedback or issue text.

    - Process each row independently and preserve ordering.
    - Return exactly one label per row: positive, 'negative', neutral, mixed, or 'unknown'.
    - 'negative': complaint, failure, error, dissatisfaction, delay, refund, blocked experience, or explicit frustration.
    - positive: satisfaction, appreciation, successful resolution, gratitude, or clearly favorable experience.
    - neutral: factual status reporting without clear positive or negative sentiment.
    - mixed: explicit positive and negative signals coexist in the same text.
    - 'unknown': the text is too ambiguous, too short, or too fragmentary to classify reliably.
    - Prefer neutral over negative when the text only reports status or handling progress without explicit dissatisfaction.
    - Do not invent context beyond each row.

    ## Rows

    {{rows_json}}
    `,
  },
];
