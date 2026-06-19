import { useMemo, useState } from "react";
import { Search } from "lucide-react";

// 시안 「처리 현황 대시보드 (키워드 분석 탭 추가)」의 "절에서 추출된 키워드" 표.
// 절(clause)마다 추출된 키워드를 배지로 보여준다. 키워드 추출 API가 아직 없어
// 예시 데이터로 하드코딩한다 — API가 생기면 이 컴포넌트만 교체하면 된다.

interface KeywordClauseRow {
  clause: string;
  keywords: string[];
}

const ROWS: KeywordClauseRow[] = [
  {
    clause: "드론쇼와 미디어아트 공연이 정말 환상적이었어요",
    keywords: ["드론쇼", "미디어아트", "공연"],
  },
  {
    clause: "체험 부스가 다양해서 아이들이 한참을 즐거워했습니다",
    keywords: ["체험 부스", "아이들"],
  },
  {
    clause: "강변 야경과 조명 분위기가 너무 좋았어요",
    keywords: ["강변 야경", "조명", "분위기"],
  },
  {
    clause: "먹거리 장터는 종류는 많은데 가격이 조금 비쌌어요",
    keywords: ["먹거리 장터", "가격"],
  },
  {
    clause: "주차장이 협소해서 30분 넘게 헤맸습니다",
    keywords: ["주차장", "협소"],
  },
  {
    clause: "사람이 너무 많아서 이동하기가 불편했어요",
    keywords: ["혼잡", "이동"],
  },
  {
    clause: "전통 공예 체험은 그냥 무난한 편이었습니다",
    keywords: ["전통 공예", "체험"],
  },
  {
    clause: "안내 요원분들이 친절하게 길을 알려주셨어요",
    keywords: ["안내 요원", "친절"],
  },
];

export default function KeywordClauseTable() {
  const [search, setSearch] = useState("");

  const shown = useMemo(() => {
    const q = search.trim();
    if (!q) return ROWS;
    return ROWS.filter(
      (r) => r.clause.includes(q) || r.keywords.some((k) => k.includes(q)),
    );
  }, [search]);

  return (
    <div className="overflow-hidden rounded-2xl border border-zinc-100 bg-white shadow-sm">
      <div className="flex flex-wrap items-center justify-between gap-2 border-b border-zinc-50 px-4 py-3">
        <div className="flex items-center gap-2">
          <div className="text-[15px] font-bold text-zinc-900">
            키워드 추출 결과
          </div>
          <span className="text-xs text-zinc-400">
            {ROWS.length}건
          </span>
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

      <div className="overflow-x-auto">
        <table className="w-full table-fixed text-sm">
          <thead>
            <tr className="border-b border-zinc-100 bg-zinc-50/70 text-left text-xs font-semibold text-zinc-500">
              <th className="px-4 py-3">절</th>
              <th className="px-4 py-3">키워드</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {shown.length === 0 ? (
              <tr>
                <td
                  colSpan={2}
                  className="py-8 text-center text-sm text-zinc-400"
                >
                  검색 결과가 없습니다
                </td>
              </tr>
            ) : (
              shown.map((r, i) => (
                <tr key={i} className="transition-colors hover:bg-zinc-50/60">
                  <td className="px-4 py-3 align-top text-[13px] leading-relaxed text-zinc-700">
                    {r.clause}
                  </td>
                  <td className="px-4 py-3 align-top">
                    <div className="flex flex-wrap gap-1.5">
                      {r.keywords.map((k) => (
                        <span
                          key={k}
                          className="rounded-full bg-violet-50 px-2.5 py-1 text-xs font-bold text-violet-700"
                        >
                          {k}
                        </span>
                      ))}
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
