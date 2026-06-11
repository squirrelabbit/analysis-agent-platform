import { useVersion } from "../hooks/version.query";
import type { BuildJobType } from "@/shared/types/common";
import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { List } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { buildLabel } from "@/shared/constants/buildLabels";
import { fmtDate, formatFileSize } from "@/shared/utils/format";
import { useVersionParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { useDataset } from "@/features/datasets/hooks/dataset.query";
import PipelineCard from "../components/PipelineCard";
import { useVersionsWithNumber } from "../redesign/useVersionsWithNumber";
import CleanTab from "../components/tabs/CleanTab";
import GenuinenessTab from "../components/tabs/GenuinenessTab";
import { ClauseTab } from "../components/tabs/ClauseTab";
import { KeywordTab } from "../components/tabs/KeywordTab";

export default function VersionDetailPage() {
  const navigate = useNavigate();
  const { projectId, datasetId, versionId } = useVersionParams();
  const { data: project } = useProjectDetail(projectId);
  const { data: dataset } = useDataset();
  const { data: version } = useVersion();
  const { data: versions = [] } = useVersionsWithNumber();
  const [tab, setTab] = useState<BuildJobType>("clean");

  if (!version) return null;

  const current = versions.find((v) => v.id === versionId);

  const builds = ["clean", "doc_genuineness", "clause_label", 'clause_keywords'] as BuildJobType[];

  return (
    <div className="p-8">
      {/* 상단 breadcrumb 헤더 — 데이터셋 계열 페이지 공용 디자인 */}
      <div className="mb-6">
        <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            {
              label: project?.name ?? "프로젝트",
              to: `/projects/${projectId}/datasets`,
            },
            {
              label: dataset?.name ?? "데이터셋",
              to: `/projects/${projectId}/datasets/${datasetId}/versions`,
            },
            { label: "데이터 처리 현황" },
          ]}
        />
      </div>

      {/* 상단 영역 — 타겟 버전 요약 바 */}
      {current && (
        <div className="mb-6 flex flex-wrap items-center gap-3 rounded-2xl border border-slate-200 bg-white px-5 py-3.5 shadow-sm">
          <span className="text-sm font-medium text-slate-500">타겟 버전</span>
          <span className="text-[15px] font-bold text-slate-900">
            {current.originalFilename}
          </span>
          <span className="rounded-md bg-violet-100 px-2 py-0.5 text-xs font-bold text-violet-700">
            v{current.versionNumber}
          </span>
          {current.isActive && (
            <span className="rounded-full bg-emerald-100 px-2.5 py-0.5 text-xs font-semibold text-emerald-700">
              활성 버전
            </span>
          )}
          <span className="text-slate-300">·</span>
          <span className="text-sm text-slate-500">
            {fmtDate(current.createdAt)} · {current.rowCount.toLocaleString()}건
            · {formatFileSize(current.byteSize)}
          </span>
          <button
            onClick={() =>
              navigate(`/projects/${projectId}/datasets/${datasetId}/versions`)
            }
            className="ml-auto inline-flex items-center gap-1.5 rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm font-semibold text-slate-600 transition-colors hover:bg-slate-50"
          >
            <List className="h-4 w-4" />
            버전 목록
          </button>
        </div>
      )}

      <div className="grid lg:grid-cols-4 grid-cols-2 gap-4">
        {builds.map((build) => (
          <PipelineCard key={build} versionId={version.id} type={build} />
        ))}
      </div>
      <Tabs value={tab} onValueChange={(v) => setTab(v as BuildJobType)}>
        <TabsList variant="line" className="mt-6 mb-3">
          {builds.map((build) => (
            <TabsTrigger
              key={build}
              value={build}
              className="h-auto flex-none gap-2 rounded-none border-0 bg-transparent px-4 pb-3 pt-2.5 text-[14.5px] font-semibold text-zinc-400 transition-colors after:bg-violet-600 hover:text-zinc-600 data-[state=active]:bg-transparent data-[state=active]:text-violet-700 data-[state=active]:shadow-none"
            >
              {buildLabel(build)}
            </TabsTrigger>
          ))}
        </TabsList>

        {/* 각 탭이 로딩/미실행/진행중/결과 상태를 자체 처리하므로 항상 렌더 */}
        <TabsContent value="clean" className="animate-in fade-in duration-300">
          <CleanTab />
        </TabsContent>
        <TabsContent
          value="doc_genuineness"
          className="animate-in fade-in duration-300"
        >
          <GenuinenessTab />
        </TabsContent>
        <TabsContent
          value="clause_label"
          className="animate-in fade-in duration-300"
        >
          <ClauseTab />
        </TabsContent>
        <TabsContent
          value="clause_keywords"
          className="animate-in fade-in duration-300"
        >
          <KeywordTab />
        </TabsContent>
      </Tabs>
    </div>
  );
}
