import { Badge } from "@/components/ui/badge";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import type {
  BuildStageResult,
  Stage,
} from "@/features/dataset/types/datasetVersion";
import { cn } from "@/lib/utils";
import { CheckCircle2, Clock, Lock } from "lucide-react";
import AnalysisDialog from "./forms/AnalysisDialog";
import { useParams } from "react-router-dom";

export interface FormProps<T> {
  formId: string;
  onSubmit: (data: T) => Promise<void>;
  onSuccess: () => void;
}

// ── 스테이지 메타 (라벨, 설명, 그룹) ─────────────────────────────────────────
const STAGE_META: Record<
  Stage,
  {
    label: string;
    desc: string;
    group: string;
    isLLM?: boolean;
    type: Stage,
    isParallel?: boolean;
  }
> = {
  clean: {
    label: "데이터 정제 (Clean)",
    desc: "노이즈·중복 제거, 텍스트 컬럼 병합.",
    group: "기본 빌드",
    type: 'clean'
  },
  docGenuineness: {
    label: "문서 품질 검증 전처리 (Prepare)",
    desc: "전체 데이터 LLM 전처리. 비용이 발생합니다.",
    group: "LLM 전처리",
    type: "docGenuineness",
    isLLM: true,
  },
  clauseLabel: {
    label: "감성 분석",
    desc: "문서별 긍/부정/중립 레이블링. 비용이 발생합니다.",
    group: "분석 — 병렬 실행 가능",
    type: "clauseLabel",
    isParallel: true,
  },
};

const STATUS_CONFIG = {
  ready: {
    label: "준비됨",
    badgeClass: "bg-green-50 text-green-700 border-green-200",
    borderClass: "border-l-green-500",
    iconBg: "bg-green-50",
  },
  queued: {
    label: "대기중",
    badgeClass: "bg-amber-50 text-amber-700 border-amber-200",
    borderClass: "border-l-amber-400",
    iconBg: "bg-amber-50",
  },
  stale: {
    label: "갱신 필요",
    badgeClass: "bg-amber-50 text-amber-700 border-amber-200",
    borderClass: "border-l-amber-400",
    iconBg: "bg-amber-50",
  },
  not_requested: {
    label: "미요청",
    badgeClass: "bg-zinc-100 text-zinc-500 border-zinc-200",
    borderClass: "border-l-zinc-300",
    iconBg: "bg-zinc-100",
  },
  "": {
    label: "미요청",
    badgeClass: "bg-zinc-100 text-zinc-500 border-zinc-200",
    borderClass: "border-l-zinc-300",
    iconBg: "bg-zinc-100",
  },
  failed: {
    label: "실패",
    badgeClass: "bg-red-50 text-red-700 border-red-200",
    borderClass: "border-l-red-400",
    iconBg: "bg-red-50"

  }
};

export function StageIcon({
  status,
  className,
}: {
  status: string;
  className?: string;
}) {
  const base = "w-3.5 h-3.5";
  const cls = className ?? base;

  switch (status) {
    case "ready":
      return <CheckCircle2 className={`${cls} text-emerald-500`} />;
    case "stale":
      return <Lock className={`${cls} text-muted-foreground`} />;
    default:
      return <Clock className={`${cls} text-muted-foreground`} />;
  }
}

export function BuildStageCard({
  id,
  stage,
  buildStage,
}: {
  id: string,
  stage: Stage,
  buildStage: BuildStageResult;
}) {
  const { projectId, datasetId } = useParams()
  const { status } = buildStage;

  const meta = STAGE_META[stage];
  const config = STATUS_CONFIG[status as "ready" | "stale" | "not_requested" | "queued" | "" | "failed"];

  if (!projectId || !datasetId ) return null
  return (
    <Item className={cn("border-l-2 bg-white", config.borderClass)}>
      <ItemMedia className={cn("p-2 rounded-lg", config.iconBg)}>
        <StageIcon status={status} />
      </ItemMedia>
      <ItemContent>
        <ItemTitle>{meta.label ?? stage}</ItemTitle>
        <ItemDescription>{meta.desc}</ItemDescription>
      </ItemContent>
      <ItemActions className="flex-col">
        <Badge className={cn(config.badgeClass)}>{config.label}</Badge>
          <AnalysisDialog
            versionId={id}
            formId={`${stage}-form`}
            stage={stage}
            status={status}
            >
          </AnalysisDialog>
      </ItemActions>
    </Item>
  );
}
