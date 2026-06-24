import type { LucideIcon } from "lucide-react";
import {
  DiscAlbum,
  FileText,
  Film,
  Image,
  MessageCircle,
  MessagesSquare,
  Newspaper,
  SquarePlay,
} from "lucide-react";
import { BarTrack } from "@/components/common/charts";
import { fmt, pct, type ScopeData } from "../mock";
import {
  Block,
  BlockTitle,
  CardInsight,
  DataTableMini,
} from "./primitives";

// 채널 key → 아이콘 (mock 데이터는 JSX를 담지 않으므로 여기서 매핑).
const CHANNEL_ICON: Record<string, LucideIcon> = {
  insta: DiscAlbum,
  blog: FileText,
  news: Newspaper,
  comm: MessagesSquare,
  youtube: SquarePlay,
  post: Image,
  reels: Film,
  comment: MessageCircle,
};

export default function ChannelSection({ data }: { data: ScopeData }) {
  const { channels, docTotal } = data;
  const max = Math.max(...channels.map((c) => c.n), 1);
  const top2 = pct(channels[0].n + (channels[1]?.n ?? 0), docTotal);

  return (
    <Block>
      <BlockTitle
        title="채널별 진성 문서 분포"
        sub={`진성 문서를 채널별 비율로 나누어 봅니다. 총 ${fmt(docTotal)}건 · 채널별 문서수와 비율`}
        unit="doc"
      />

      <div className="mt-4 grid grid-cols-1 gap-7 lg:grid-cols-[1fr_284px] items-center">
        {/* 막대 */}
        <div className="flex flex-col gap-5">
          {channels.map((c) => {
            const Icon = CHANNEL_ICON[c.key] ?? FileText;
            return (
              <div
                key={c.key}
                className="grid grid-cols-[110px_1fr_120px] items-center gap-6"
              >
                <div className="flex min-w-0 items-center gap-2.5 text-[13.5px] font-semibold text-zinc-800">
                  <span className="grid h-6.5 w-6.5 shrink-0 place-items-center rounded-lg bg-violet-50 text-violet-600">
                    <Icon className="h-3.75 w-3.75" />
                  </span>
                  <span className="truncate">{c.name}</span>
                </div>
                <BarTrack
                  className="h-5.5 !rounded-md"
                  percent={(c.n / docTotal) * 100}
                  fillClassName="!rounded-md bg-linear-to-r from-violet-500 to-violet-600 transition-all duration-500"
                />
                <div className="flex items-baseline gap-2 tabular-nums">
                  <span className="text-[15px] font-extrabold text-zinc-900">
                    {fmt(c.n)}건
                  </span>
                  <span className="text-[12.5px] font-bold text-zinc-400">
                    {pct(c.n, docTotal)}%
                  </span>
                </div>
              </div>
            );
          })}
        </div>

        {/* 표 */}
        <DataTableMini
          head={
            <thead>
              <tr className="bg-zinc-50/70 text-[11px] font-bold text-zinc-400">
                <th className="px-3 py-2.5 text-left">채널</th>
                <th className="px-3 py-2.5 text-right">문서수</th>
                <th className="px-3 py-2.5 text-right">비율</th>
              </tr>
            </thead>
          }
        >
          <tbody>
            {channels.map((c) => (
              <tr key={c.key} className="border-t border-zinc-100">
                <td className="px-3 py-2.25 font-semibold text-zinc-800">
                  {c.name}
                </td>
                <td className="px-3 py-2.25 text-right font-bold tabular-nums text-zinc-900">
                  {fmt(c.n)}
                </td>
                <td className="px-3 py-2.25 text-right tabular-nums text-zinc-500">
                  {pct(c.n, docTotal)}%
                </td>
              </tr>
            ))}
          </tbody>
          <tfoot>
            <tr className="border-t border-zinc-200 bg-zinc-50/70 font-extrabold text-zinc-900">
              <td className="px-3 py-2.5">합계</td>
              <td className="px-3 py-2.5 text-right tabular-nums">
                {fmt(docTotal)}
              </td>
              <td className="px-3 py-2.5 text-right tabular-nums">100%</td>
            </tr>
          </tfoot>
        </DataTableMini>
      </div>

      <CardInsight>
        <b>{channels[0].name}</b> 채널 비중이 {pct(channels[0].n, docTotal)}%로
        가장 높고, 상위 2개 채널이 전체 진성 문서의 <b>{top2}%</b>를 차지합니다.
      </CardInsight>
    </Block>
  );
}
