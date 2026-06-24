import { useMemo, useState } from "react";
import {
  Calendar,
  CheckCircle2,
  ChevronDown,
  Database,
  Info,
  Layers,
  ListChecks,
  Sparkles,
  Table2,
} from "lucide-react";
import type { LucideIcon } from "lucide-react";
import { useProjectId } from "@/hooks/useProjectId";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { cn } from "@/lib/utils";
import { DATASETS, fmt, type DatasetKey, type ScopeKey } from "../mock";
import { SectionLabel, ScopePill, Block } from "../components/primitives";
import ChannelSection from "../components/ChannelSection";
import SentimentSection from "../components/SentimentSection";
import TypeSection from "../components/TypeSection";
import TypeSentimentSection from "../components/TypeSentimentSection";
import KeywordSection from "../components/KeywordSection";

// 데이터 기초 분석 보고서 페이지.
// 전처리(정제·진성·절 라벨링)가 완료된 데이터의 기본 분석 결과를 보여준다.
// 확인 필요: 집계 API가 아직 없어 mock(../mock.ts)으로 렌더한다.

function OverviewCell({
  icon: Icon,
  label,
  children,
}: {
  icon: LucideIcon;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <div className="bg-white px-4 py-3.5">
      <div className="inline-flex items-center gap-1.5 text-[12px] font-semibold text-zinc-400">
        <Icon className="h-3.25 w-3.25" />
        {label}
      </div>
      <div className="mt-1.75 text-[15px] font-bold tracking-tight text-zinc-900">
        {children}
      </div>
    </div>
  );
}

function DocBlock({
  scope,
  label,
  value,
  desc,
}: {
  scope: "all" | "recent";
  label: string;
  value: number;
  desc: string;
}) {
  const isRecent = scope === "recent";
  return (
    <div className="relative overflow-hidden rounded-xl border border-zinc-100 bg-white p-4.5 shadow-sm">
      <div className="flex justify-between items-center">

      <div className="text-[12.5px] font-semibold text-zinc-600">
        {label}
      </div>
      <span
        className={cn(
          "absolute inset-y-0 left-0 w-0.75",
          isRecent ? "bg-violet-500" : "bg-zinc-300",
        )}
      />
      <span
        className={cn(
          "inline-flex items-center gap-1.5 rounded-full px-2 py-0.75 text-[11px] font-bold",
          isRecent
            ? "bg-violet-50 text-violet-600"
            : "bg-zinc-100 text-zinc-500",
        )}
      >
        {isRecent ? "최근 연도" : "전체 기간"}
      </span>
      </div>

      <div className="text-3xl my-2  font-extrabold leading-none tracking-tight tabular-nums text-zinc-900">
        {fmt(value)}
      </div>
      <div className="inline-flex items-center gap-1.5 text-[11.5px] font-medium text-zinc-400">
        <Calendar className="h-3 w-3" />
        {desc}
      </div>
    </div>
  );
}

export const AnalyticsPage = () => {
  const { projectId } = useProjectId();
  const { data: project } = useProjectDetail(projectId);

  const [dataset, setDataset] = useState<DatasetKey>("festival_sns");
  const [scope, setScope] = useState<ScopeKey>("recent");

  const ds = DATASETS[dataset];
  const cur = useMemo(() => ds[scope], [ds, scope]);
  const scopeLabel = cur.label;

  return (
    <div className="mx-auto w-full max-w-[1180px] px-8 py-6 pb-20">
      <Breadcrumbs
        items={[
          { label: project?.name ?? "프로젝트" },
          { label: "데이터 기초 분석" },
        ]}
      />

      {/* 헤더 */}
      <div className="mt-3.5 flex flex-wrap items-start gap-4">
        <div className="min-w-0 flex-1">
          <h1 className="text-2xl font-extrabold tracking-tight text-zinc-900">
            데이터 기초 분석 보고서
          </h1>
          <p className="mt-1.5 text-sm text-zinc-500">
            전처리(정제 · 진성 분석 · 절 라벨링)가 완료된 분석 데이터의 기본
            분석 결과입니다.
          </p>
        </div>

        {/* 데이터셋 선택 */}
        <label className="relative inline-flex h-9.5 shrink-0 items-center gap-2 rounded-xl border border-zinc-200 bg-white pl-3 pr-9 transition focus-within:border-violet-500 focus-within:ring-2 focus-within:ring-violet-100 hover:border-zinc-300">
          <Database className="h-4 w-4 shrink-0 text-violet-600" />
          <select
            value={dataset}
            onChange={(e) => setDataset(e.target.value as DatasetKey)}
            className="cursor-pointer appearance-none bg-transparent font-mono text-[13px] font-semibold text-zinc-900 outline-none"
          >
            {(Object.keys(DATASETS) as DatasetKey[]).map((key) => (
              <option key={key} value={key}>
                {DATASETS[key].label}
              </option>
            ))}
          </select>
          <ChevronDown className="pointer-events-none absolute right-3 h-3.75 w-3.75 text-zinc-400" />
        </label>

        <span className="inline-flex h-7.5 shrink-0 items-center gap-1.5 rounded-full bg-emerald-50 px-3 text-[12.5px] font-bold text-emerald-700">
          <CheckCircle2 className="h-3.5 w-3.5" />
          전처리 완료
        </span>
      </div>

      {/* 조회 기간 */}
      {/* <div className="mt-5 flex flex-wrap items-center gap-3.5 rounded-2xl border border-zinc-100 bg-white px-4.5 py-3.5 shadow-sm">
        <span className="inline-flex items-center gap-2 text-[13px] font-bold text-zinc-900">
          <Calendar className="h-4 w-4 text-zinc-400" />
          조회 기간
        </span>
        <div className="inline-flex gap-0.5 rounded-[10px] bg-zinc-100 p-0.75">
          {(
            [
              { key: "recent" as const, label: `최근 연도 (${ds.recentYear})` },
              { key: "all" as const, label: `전체 기간 (${ds.allShort})` },
            ]
          ).map((seg) => (
            <button
              key={seg.key}
              type="button"
              onClick={() => setScope(seg.key)}
              className={cn(
                "rounded-[7px] px-3.5 py-1.75 text-[12.5px] font-semibold transition",
                scope === seg.key
                  ? "bg-white text-violet-600 shadow-sm"
                  : "text-zinc-500 hover:text-zinc-800",
              )}
            >
              {seg.label}
            </button>
          ))}
        </div>
        <span className="inline-flex items-center gap-2 text-[13px] font-semibold text-zinc-500">
          데이터 일자{" "}
          <span className="font-mono text-zinc-900">{cur.range}</span>
        </span>
        <span className="ml-auto inline-flex items-center gap-1.5 text-[12px] font-medium text-zinc-400">
          <Info className="h-3.5 w-3.5" />
          기본값: 최근 연도 데이터만 조회
        </span>
      </div> */}

      <div className="mt-10 space-y-5">
        {/* 2. 분석 개요 */}
        <Block className="p-0!">
          <div className="grid grid-cols-2 gap-px overflow-hidden rounded-2xl bg-zinc-100 sm:grid-cols-3">
            <OverviewCell icon={Database} label="데이터셋">
              {ds.label}{" "}
              <span className="text-[12.5px] font-semibold text-zinc-400">
                · {ds.ver}
              </span>
            </OverviewCell>
            <OverviewCell icon={Calendar} label="분석 기간">
              {ds.period}{" "}
              <span className="text-[12.5px] font-semibold text-zinc-400">
                · {ds.periodYears}
              </span>
            </OverviewCell>
            <OverviewCell icon={Layers} label="수집 채널">
              {ds.channelCount}개 채널
            </OverviewCell>
            {/* <OverviewCell icon={Sparkles} label="분석 모델">
            <code className="font-mono text-[13px] text-violet-600">
              LLOA-MAX
            </code>
          </OverviewCell>
          <OverviewCell icon={Table2} label="분석 단위">
            문서 · 절(clause)
          </OverviewCell>
          <OverviewCell icon={ListChecks} label="전처리 단계">
            정제 · 진성 · 절 라벨링 
          </OverviewCell> */}
          </div>
        </Block>

        {/* 3. 문서 개요 */}
        <div className="grid grid-cols-2 gap-3.5 lg:grid-cols-4">
          <DocBlock
            scope="all"
            label="진성 문서수"
            value={ds.all.docTotal}
            desc={`${ds.all.range} · ${ds.periodYears} 전체`}
          />
          <DocBlock
            scope="recent"
            label="진성 문서수"
            value={ds.recent.docTotal}
            desc={ds.recent.range}
          />
          <DocBlock
            scope="all"
            label="절(clause) 수"
            value={ds.all.clauseTotal}
            desc={`진성 문서에서 추출 · ${ds.periodYears} 전체`}
          />
          <DocBlock
            scope="recent"
            label="절(clause) 수"
            value={ds.recent.clauseTotal}
            desc={`${ds.recentYear}년 진성 문서 기준`}
          />
        </div>

        <ChannelSection data={cur} />

        {/* 5. 감성 분포 */}
        <SentimentSection data={cur} />

        {/* 6. 유형 분포 */}
        <TypeSection data={cur} />

        {/* 7. 유형별 감성 비중 */}
        <TypeSentimentSection data={cur} />

        {/* 8. 키워드 분석 */}
        <KeywordSection kwPos={cur.kwPos} kwNeg={cur.kwNeg} />
      </div>
    </div>
  );
};
