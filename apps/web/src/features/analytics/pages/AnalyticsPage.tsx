import { useMemo, useState } from "react";
import { CheckCircle2, ChevronDown, Database } from "lucide-react";
import { useProjectId } from "@/hooks/useProjectId";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import BasicReportTemplate from "@/features/reports/components/basicTemplate/BasicReportTemplate";
import { DATASETS, type DatasetKey, type ScopeKey } from "../mock";
import { buildBasicReport } from "../buildBasicReport";

// 데이터 기초 분석 보고서 페이지.
// 전처리(정제·진성·절 라벨링)가 완료된 데이터의 기본 분석 결과를, 기본 템플릿 계약
// (report_basic_template.sample.md)을 따르는 공통 컴포넌트 BasicReportTemplate로 렌더한다.
// 확인 필요: 집계 API가 아직 없어 mock(../mock.ts)을 ../buildBasicReport로 템플릿 모양으로
// 변환해 공급한다. API 준비 시 buildBasicReport 대신 /reports/from_template 응답을 넘기면 된다.

export const AnalyticsPage = () => {
  const { projectId } = useProjectId();
  const { data: project } = useProjectDetail(projectId);

  const [dataset, setDataset] = useState<DatasetKey>("festival_sns");
  // 조회 범위 토글은 현재 비노출 — 기본값(최근 연도)로 고정한다.
  const scope: ScopeKey = "recent";

  const ds = DATASETS[dataset];
  const report = useMemo(() => buildBasicReport(ds, scope), [ds, scope]);

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

      {/* 기본 분석 보고서 — 템플릿 계약 기반 공통 렌더러 */}
      <BasicReportTemplate report={report} className="mt-10" />
    </div>
  );
};
