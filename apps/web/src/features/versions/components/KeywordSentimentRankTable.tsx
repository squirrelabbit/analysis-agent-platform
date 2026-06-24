import { ThumbsDown, ThumbsUp } from "lucide-react";

// 키워드 분석 탭의 "키워드 추출 결과" 카드 위에 놓는 긍/부정 키워드 순위 표.
// 왼쪽=긍정, 오른쪽=부정. 컬럼: 순위 / 키워드 / 빈도. 빈도 내림차순 전체를 보여주되
// 10개를 넘으면 표 본문만 스크롤한다(헤더는 sticky 고정). (silverone 2026-06-23)
//
// 확인 필요: 키워드별 긍/부정 순위 API는 아직 없어 아래 MOCK 데이터로 하드코딩한다.
// 추출 API가 준비되면 props(summary)로 교체.

// 10행이 보이는 본문 높이. 행 높이(약 36px)에 맞춘 근사값.
const VISIBLE_ROWS = 10;
const ROW_HEIGHT = 36;

type RankRow = { keyword: string; count: number };

// ── MOCK 데이터 (API 준비 전 임시) ──────────────────────────────
const MOCK_POSITIVE: RankRow[] = [
  { keyword: "친절", count: 482 },
  { keyword: "맛집", count: 451 },
  { keyword: "분위기", count: 397 },
  { keyword: "야경", count: 356 },
  { keyword: "가성비", count: 312 },
  { keyword: "재방문", count: 289 },
  { keyword: "포토존", count: 254 },
  { keyword: "공연", count: 231 },
  { keyword: "체험", count: 207 },
  { keyword: "주차편리", count: 188 },
  { keyword: "깨끗", count: 164 },
  { keyword: "프로그램", count: 142 },
  { keyword: "전통", count: 121 },
  { keyword: "야시장", count: 103 },
  { keyword: "추천", count: 87 },
];

const MOCK_NEGATIVE: RankRow[] = [
  { keyword: "혼잡", count: 374 },
  { keyword: "대기줄", count: 341 },
  { keyword: "주차난", count: 298 },
  { keyword: "비쌈", count: 263 },
  { keyword: "불친절", count: 219 },
  { keyword: "소음", count: 192 },
  { keyword: "쓰레기", count: 168 },
  { keyword: "더위", count: 147 },
  { keyword: "협소", count: 129 },
  { keyword: "안내부족", count: 111 },
  { keyword: "화장실", count: 94 },
  { keyword: "교통", count: 78 },
  { keyword: "줄서기", count: 61 },
];

function RankTable({
  title,
  rows,
  tone,
  icon: Icon,
}: {
  title: string;
  rows: RankRow[];
  tone: "positive" | "negative";
  icon: typeof ThumbsUp;
}) {
  const accent =
    tone === "positive"
      ? "text-emerald-600 bg-emerald-50"
      : "text-rose-600 bg-rose-50";

  return (
    <div className="overflow-hidden rounded-2xl border border-zinc-100 bg-white shadow-sm">
      <div className="flex items-center gap-2 border-b border-zinc-50 px-4 py-3">
        <span className={`grid h-6 w-6 place-items-center rounded-lg ${accent}`}>
          <Icon className="h-3.5 w-3.5" />
        </span>
        <div className="text-[15px] font-bold text-zinc-900">{title}</div>
        <span className="text-xs text-zinc-400">{rows.length}개</span>
      </div>

      <div
        className="overflow-y-auto"
        style={{ maxHeight: VISIBLE_ROWS * ROW_HEIGHT }}
      >
        <table className="w-full table-fixed text-sm">
          <thead className="sticky top-0 z-10">
            <tr className="border-b border-zinc-100 bg-zinc-50/95 text-left text-xs font-semibold text-zinc-500 backdrop-blur">
              <th className="w-14 px-4 py-2.5">순위</th>
              <th className="px-4 py-2.5">키워드</th>
              <th className="w-20 px-4 py-2.5 text-right">빈도</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {rows.length === 0 ? (
              <tr>
                <td
                  colSpan={3}
                  className="py-8 text-center text-sm text-zinc-400"
                >
                  키워드가 없습니다
                </td>
              </tr>
            ) : (
              rows.map((r, i) => (
                <tr
                  key={`${r.keyword}-${i}`}
                  className="transition-colors hover:bg-zinc-50/60"
                >
                  <td className="px-4 py-2 text-xs font-semibold text-zinc-400">
                    {i + 1}
                  </td>
                  <td className="px-4 py-2 font-medium text-zinc-800">
                    {r.keyword}
                  </td>
                  <td className="px-4 py-2 text-right tabular-nums text-zinc-600">
                    {r.count.toLocaleString()}
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

export default function KeywordSentimentRankTable() {
  return (
    <div className="grid grid-cols-1 gap-3.5 sm:grid-cols-2">
      <RankTable
        title="긍정 키워드"
        rows={MOCK_POSITIVE}
        tone="positive"
        icon={ThumbsUp}
      />
      <RankTable
        title="부정 키워드"
        rows={MOCK_NEGATIVE}
        tone="negative"
        icon={ThumbsDown}
      />
    </div>
  );
}
