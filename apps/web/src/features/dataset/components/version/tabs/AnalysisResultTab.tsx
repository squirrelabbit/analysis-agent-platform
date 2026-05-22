import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import type {
  BuildStageResult,
  ClauseLabelSummary,
  CleanSummary,
  DatasetVersionDetail,
  DocGenuinenessSummary,
  Stage,
} from "@/features/dataset/types/datasetVersion";
import { useParams } from "react-router-dom";
import { CleanSummaryTab } from "./analysis-result/CleanSummaryTab";
import { DocGenuinenessTab } from "./analysis-result/DocGenuinenessTab";
import { ClauseLabelTab } from "./analysis-result/ClauseLabelTab";


// ── 상태 뱃지 ─────────────────────────────────────────────────────────────────
const STATUS_DOT: Record<string, string> = {
  ready: "bg-emerald-500",
  enqueue: "bg-amber-400",
  stale: "bg-amber-400",
  not_requested: "bg-zinc-300",
  failed: "bg-red-400",
  "": "bg-zinc-300",
};

const STAGE_LABEL: Record<string, string> = {
  clean: "clean",
  docGenuineness: "docGenuineness",
  clauseLabel: "clauseLabel",
};

const STAGE_ORDER = ["clean", "docGenuineness", "clauseLabel"];

// ── AnalysisResultTab ─────────────────────────────────────────────────────────
export function AnalysisResultTab({
  detail,
}: {
  detail: DatasetVersionDetail;
}) {
  const { projectId, datasetId } = useParams();
  const { clean, docGenuineness, clauseLabel } = detail;

  const stages: {
    stage: Stage;
    buildStage: BuildStageResult;
    summary?: any;
  }[] = [
    { stage: "clean", buildStage: clean },
    { stage: "docGenuineness", buildStage: docGenuineness },
    { stage: "clauseLabel", buildStage: clauseLabel },
  ];

  const stageMap = Object.fromEntries(stages.map((s) => [s.stage, s]));

  const orderedStages = STAGE_ORDER.map((key) => stageMap[key]).filter(Boolean);

  const defaultTab =
    stages.find(({ buildStage }) => buildStage.status === "ready")?.stage ??
    stages[0]?.stage;

  if (!projectId || !datasetId) return null;

  return (
    <Tabs defaultValue={defaultTab} className="flex flex-col gap-4">
      {/* 스테이지 선택 탭 */}
      <TabsList className="h-auto bg-transparent p-0 flex gap-1.5 flex-wrap justify-start">
        {orderedStages.map((stage) => (
          <TabsTrigger
            key={stage.stage}
            value={stage.stage}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-full border border-zinc-200 text-xs font-medium text-zinc-500
              data-[state=active]:bg-indigo-50 data-[state=active]:border-indigo-400 data-[state=active]:text-indigo-700
              data-[state=inactive]:bg-white hover:border-indigo-200 hover:text-indigo-500 transition-colors"
          >
            <span
              className={`w-1.5 h-1.5 rounded-full shrink-0 ${STATUS_DOT[stage.buildStage.status]}`}
            />
            {STAGE_LABEL[stage.stage] ?? stage.stage}
          </TabsTrigger>
        ))}
      </TabsList>

      {/* 각 스테이지 결과 */}
      {orderedStages.map(({ stage, buildStage }) => (
        <TabsContent key={stage} value={stage} className="mt-0">
          <StageResultContent
            stage={stage}
            buildStage={buildStage}
          />
        </TabsContent>
      ))}
    </Tabs>
  );
}

// ── StageResultContent — 스테이지별 라우팅 ────────────────────────────────────
function StageResultContent({
  stage,
  buildStage,
}: {
  stage: Stage;
  buildStage: BuildStageResult;
}) {
  if (!buildStage.summary)
    return <EmptyResult message={`${stage} 결과가 없습니다`} />;
  switch (stage) {
    case "clean":
      return <CleanSummaryTab summary={buildStage.summary as CleanSummary} />;
    case "docGenuineness":
      return (
        <DocGenuinenessTab
          summary={buildStage.summary as DocGenuinenessSummary}
        />
      );
    case "clauseLabel":
      return (
        <ClauseLabelTab summary={buildStage.summary as ClauseLabelSummary} />
      );
    default:
      return <EmptyResult message={`${stage} 결과가 없습니다`} />;
  }
}

// ── 공통 빈 상태 ──────────────────────────────────────────────────────────────
export function EmptyResult({ message }: { message?: string }) {
  return (
    <div className="flex flex-col items-center justify-center gap-2 py-12 text-zinc-400">
      <div className="w-9 h-9 rounded-xl bg-zinc-100 flex items-center justify-center">
        <svg
          className="w-5 h-5"
          viewBox="0 0 18 18"
          fill="none"
          stroke="currentColor"
          strokeWidth="1.3"
        >
          <circle cx="9" cy="9" r="6" />
          <line x1="9" y1="6" x2="9" y2="9" />
          <circle cx="9" cy="12" r=".6" fill="currentColor" />
        </svg>
      </div>
      <p className="text-sm font-medium text-zinc-500">결과 없음</p>
      <p className="text-xs text-zinc-400">
        {message ?? "파이프라인 탭에서 실행하세요"}
      </p>
    </div>
  );
}
