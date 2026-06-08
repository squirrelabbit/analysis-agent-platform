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

  const builds = ["clean", "doc_genuineness", "clause_label"] as BuildJobType[];

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
            {fmtDate(current.createdAt)} · {current.rowCount.toLocaleString()}건 ·{" "}
            {formatFileSize(current.byteSize)}
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

      <div className="grid md:grid-cols-3 gap-4">
        {builds.map((build) => (
          <PipelineCard key={build} versionId={version.id} type={build} />
        ))}
      </div>
      <Tabs value={tab} onValueChange={(v) => setTab(v as BuildJobType)}>
        <TabsList className="mt-4">
          <TabsTrigger value="clean">{buildLabel("clean")}</TabsTrigger>
          <TabsTrigger value="doc_genuineness">
            {buildLabel("doc_genuineness")}
          </TabsTrigger>
          <TabsTrigger value="clause_label">
            {buildLabel("clause_label")}
          </TabsTrigger>
        </TabsList>
        {/* 각 탭이 로딩/미실행/진행중/결과 상태를 자체 처리하므로 항상 렌더 */}
        <TabsContent value="clean">
          <CleanTab />
        </TabsContent>
        <TabsContent value="doc_genuineness">
          <GenuinenessTab />
        </TabsContent>
        <TabsContent value="clause_label">
          <ClauseTab />
        </TabsContent>
      </Tabs>
    </div>
  );
}
