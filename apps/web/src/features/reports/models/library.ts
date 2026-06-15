import type { ReportSavedResult } from "./model";
import type { LibType, LibraryItem } from "./editor";
import { projectResult } from "./result";

// 보고서 보관함(saved_results) 한 건 → 에디터 뷰모델(LibraryItem).
// 결과 도메인은 projectResult로 채팅과 동일하게 투영해 result에 담는다(렌더는 채팅 카탈로그 재사용).

// recommended_view → 보관함 필터용 타입(렌더와 별개로 분류만).
const libType = (view?: string | null): LibType => {
  if (view === "evidence") return "text";
  if (view === "bar" || view === "line" || view === "diverging_bar" || view === "metric")
    return "chart";
  return "table";
};

export const savedResultToLibraryItem = (r: ReportSavedResult): LibraryItem => ({
  id: r.resultId,
  type: libType(r.display?.recommended_view),
  title: r.title,
  sub: r.createdAt?.slice(0, 10) ?? "",
  question: r.question,
  result: projectResult(r),
});
