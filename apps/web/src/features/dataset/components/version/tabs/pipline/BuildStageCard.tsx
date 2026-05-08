import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Item,
  ItemActions,
  ItemContent,
  ItemDescription,
  ItemFooter,
  ItemMedia,
  ItemTitle,
} from "@/components/ui/item";
import type { BuildStage } from "@/features/dataset/types/datasetVersion";
import { cn } from "@/lib/utils";
import { CheckCircle2, Clock, FileText, Lock } from "lucide-react";
import { PrepareAccordion, SentimentAccordion } from "./AccordionWrapper";

// ── 스테이지 메타 (라벨, 설명, 그룹) ─────────────────────────────────────────
const STAGE_META: Record<
  string,
  {
    label: string;
    desc: string;
    group: string;
    isLLM?: boolean;
    isParallel?: boolean;
  }
> = {
  source: {
    label: "소스 데이터",
    desc: "업로드된 원본 파일. 분석의 시작점입니다.",
    group: "기본 빌드",
  },
  clean: {
    label: "정제 (Clean)",
    desc: "노이즈·중복 제거, 텍스트 컬럼 병합.",
    group: "기본 빌드",
  },
  prepare: {
    label: "전처리 (Prepare)",
    desc: "전체 데이터 LLM 전처리. 비용이 발생합니다.",
    group: "LLM 전처리",
    isLLM: true,
  },
  sentiment: {
    label: "감성 분석",
    desc: "문서별 긍/부정/중립 레이블링. 비용이 발생합니다.",
    group: "분석 — 병렬 실행 가능",
    isParallel: true,
  },
  embedding: {
    label: "임베딩",
    desc: "벡터 임베딩 생성. 클러스터링의 선행 단계.",
    group: "분석 — 병렬 실행 가능",
    isParallel: true,
  },
  cluster: {
    label: "클러스터링",
    desc: "임베딩 기반 토픽 클러스터 생성.",
    group: "후처리",
  },
};

const STATUS_CONFIG = {
  ready: {
    label: "준비됨",
    badgeClass: "bg-green-50 text-green-700 border-green-200",
    borderClass: "border-l-green-500",
    iconBg: "bg-green-50",
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

export function BuildStageCard({ buildStage }: { buildStage: BuildStage }) {
  const { stage, status } = buildStage;

  const meta = STAGE_META[stage];
  const config = STATUS_CONFIG[status as "ready" | "stale" | "not_requested"];

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
        {status == "not_requested" && (
          <Button variant="outline" size="sm">
            실행
          </Button>
        )}
      </ItemActions>
      {status == "ready" && (
        <ItemFooter>
          <FileText className="w-3.5 h-3.5 text-zinc-400 shrink-0" />
          <span className="text-xs text-zinc-500 font-mono flex-1 truncate">
            result.csv
          </span>
          <div className="flex items-center gap-3 shrink-0 text-[11px] text-zinc-400">
            <span>10건</span>
            <span>16.5MB</span>
          </div>
        </ItemFooter>
      )}
      {stage === "prepare" && <PrepareAccordion />}
      {stage === "sentiment" && <SentimentAccordion />}
    </Item>
  );
}
