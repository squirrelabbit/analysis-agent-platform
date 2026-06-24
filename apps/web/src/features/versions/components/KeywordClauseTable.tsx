import { useEffect, useMemo, useState, type ReactNode } from "react";
import { Loader2, Search } from "lucide-react";
import { useClauseKeywordClauses } from "../hooks/build.query";
import { Pagination } from "./Pagination";

// 시안 「처리 현황 대시보드 (키워드 분석 탭 추가)」의 "절에서 추출된 키워드" 표.
// 절(clause)마다 추출된 키워드를 배지로 보여준다. clause_keywords?group=clause API로
// 절 중심 집계를 서버 검색(q)·페이징해 가져온다 (silverone 2026-06-19).

const PAGE_SIZE = 20;

// 절 텍스트에서 keyword 출현 위치를 <mark>로 감싼다(부분일치, 정규식 없이).
function highlightClause(text: string, kw: string) {
  if (!kw) return text;
  const out: ReactNode[] = [];
  let from = 0;
  let idx = text.indexOf(kw);
  let key = 0;
  while (idx !== -1) {
    if (idx > from) out.push(text.slice(from, idx));
    out.push(
      <mark
        key={key++}
        className="rounded bg-yellow-200 px-0.5 text-zinc-900"
      >
        {kw}
      </mark>,
    );
    from = idx + kw.length;
    idx = text.indexOf(kw, from);
  }
  out.push(text.slice(from));
  return out;
}

export default function KeywordClauseTable() {
  const [search, setSearch] = useState("");
  const [page, setPage] = useState(1);
  // 클릭한 키워드 → 그 절에서 위치 하이라이트. {row index, keyword}.
  const [selected, setSelected] = useState<{ row: number; kw: string } | null>(
    null,
  );

  const { data, isLoading, isPlaceholderData } = useClauseKeywordClauses({
    q: search.trim() || undefined,
    limit: PAGE_SIZE,
    offset: (page - 1) * PAGE_SIZE,
  });

  const rows = data?.items ?? [];
  const total = data?.total ?? 0;
  const totalPages = useMemo(
    () => Math.max(1, Math.ceil(total / PAGE_SIZE)),
    [total],
  );

  // 검색어가 바뀌면 1페이지로.
  useEffect(() => {
    setPage(1);
  }, [search]);
  // 페이지/검색이 바뀌면 행이 달라지므로 선택 하이라이트 해제.
  useEffect(() => {
    setSelected(null);
  }, [page, search]);

  return (
    <div className="overflow-hidden rounded-2xl border border-zinc-100 bg-white shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-zinc-50 px-4 py-3">
        <div className="flex items-center gap-2">
          <div className="text-[15px] font-bold text-zinc-900">
            키워드 추출 결과
          </div>
          <span className="text-xs text-zinc-400">{total.toLocaleString()}건</span>
        </div>
        <div className="relative">
          <Search className="pointer-events-none absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-zinc-400" />
          <input
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="절·키워드 검색…"
            className="h-7 w-44 rounded-lg border border-zinc-200 pl-7 pr-2.5 text-xs outline-none focus:border-violet-400"
          />
        </div>
      </div>

      <div className="relative overflow-x-auto">
        {isPlaceholderData && (
          <div className="absolute inset-0 z-10 grid place-items-center bg-white/50">
            <Loader2 className="h-5 w-5 animate-spin text-violet-400" />
          </div>
        )}
        <table className="w-full table-fixed text-sm">
          <thead>
            <tr className="border-b border-zinc-100 bg-zinc-50/70 text-left text-xs font-semibold text-zinc-500">
              <th className="px-4 py-3">절</th>
              <th className="px-4 py-3">키워드</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {isLoading ? (
              <tr>
                <td colSpan={2} className="py-10 text-center">
                  <Loader2 className="mx-auto h-5 w-5 animate-spin text-violet-400" />
                </td>
              </tr>
            ) : rows.length === 0 ? (
              <tr>
                <td
                  colSpan={2}
                  className="py-8 text-center text-sm text-zinc-400"
                >
                  {search.trim()
                    ? "검색 결과가 없습니다"
                    : "추출된 키워드가 없습니다"}
                </td>
              </tr>
            ) : (
              rows.map((r, i) => (
                <tr key={i} className="transition-colors hover:bg-zinc-50/60">
                  <td className="px-4 py-3 align-top text-[13px] leading-relaxed text-zinc-700">
                    {selected?.row === i
                      ? highlightClause(r.clause, selected.kw)
                      : r.clause}
                    {r.occurrenceCount > 1 && (
                      <span
                        className="ml-1.5 inline-block rounded bg-zinc-100 px-1.5 py-0.5 align-middle text-[11px] font-semibold text-zinc-500"
                        title={`같은 문장이 ${r.occurrenceCount}개 문서에 등장 (리포스트/복붙)`}
                      >
                        ×{r.occurrenceCount}
                      </span>
                    )}
                  </td>
                  <td className="px-4 py-3 align-top">
                    <div className="flex flex-wrap gap-1.5">
                      {r.keywords.map((k) => {
                        const on = selected?.row === i && selected.kw === k;
                        return (
                          <button
                            key={k}
                            type="button"
                            title="클릭하면 절에서 위치를 하이라이트"
                            onClick={() =>
                              setSelected(on ? null : { row: i, kw: k })
                            }
                            className={`rounded-full px-2.5 py-1 text-xs font-bold transition-colors ${
                              on
                                ? "bg-yellow-200 text-zinc-900 ring-1 ring-yellow-400"
                                : "bg-violet-50 text-violet-700 hover:bg-violet-100"
                            }`}
                          >
                            {k}
                          </button>
                        );
                      })}
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div className="border-t border-zinc-50 px-4 py-3">
          <Pagination
            page={page}
            totalPages={totalPages}
            totalCount={total}
            onPageChange={setPage}
          />
        </div>
      )}
    </div>
  );
}
