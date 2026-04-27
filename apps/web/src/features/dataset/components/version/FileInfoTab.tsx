import { cn, formatFileSize } from "@/lib/utils";
import type { DatasetVersion } from "../../types/datasetVersion";
import { CheckCircle2, Cpu, FileCode, Hash } from "lucide-react";

interface AnalysisJob {
  type: "prepare" | "sentiment" | "embedding";
  label: string;
  status: string;
  llmMode?: string;
  model?: string;
  promptVersion?: string;
}

function toAnalysisJobs(res: DatasetVersion): AnalysisJob[] {
  return [
    {
      type: "prepare",
      label: "전처리 분석",
      status: res.prepareStatus,
      llmMode: res.prepareLLMMode,
      model: res.prepareModel,
      promptVersion: res.preparePromptVersion,
    },
    {
      type: "sentiment",
      label: "감성 분석",
      status: res.sentimentStatus,
      llmMode: res.sentimentLLMMode,
      model: res.sentimentModel,
      promptVersion: res.sentimentPromptVersion,
    },
    {
      type: "embedding",
      label: "임베딩",
      status: res.embeddingStatus,
    },
  ];
}

export default function FileInfoTab(props: DatasetVersion) {
  const { recordCount, metadata } = props;
  const { byte_size } = metadata.upload || {}; // 업로드 파일 정보
  const job = toAnalysisJobs(props).find((j) => j.status !== "not_requested");

  const infos = [
    { label: "분석 유형", value: job?.label },
    { label: "레코드 수", value: recordCount.toLocaleString() },
    { label: "파일 크기", value: formatFileSize(byte_size) },
  ];

  const metas = [
    { label: "상태", value: job?.status, icon: CheckCircle2 },
    { label: "프롬프트 버전", value: job?.promptVersion, icon: FileCode },
    { label: "모델", value: job?.model, icon: Cpu },
    { label: "LLM 모드", value: job?.llmMode, icon: Hash },
  ];

  return (
    <div>
      <div className="grid grid-cols-3 gap-3 mb-5">
        {infos.map((i) => (
          <div
            key={i.label}
            className="bg-muted/50 border border-border rounded-lg px-4 py-3"
          >
            <p className="text-[10px] font-mono text-muted-foreground uppercase tracking-wide mb-1">
              {i.label}
            </p>
            <p className="text-xl font-bold text-foreground leading-none">
              {i.value}
            </p>
          </div>
        ))}
      </div>
      <div className="bg-muted/30 border border-border rounded-lg px-4 mb-5">
        {metas.map((m) => (
          <div className="flex items-center justify-between py-2.5 border-b border-border last:border-b-0">
            <div className="flex items-center gap-2 text-muted-foreground">
              <m.icon className="w-3.5 h-3.5" />
              <span className="text-[11px]">{m.label}</span>
            </div>
            <span
              className={cn(
                "font-mono text-[12px] font-semibold text-foreground",
              )}
            >
              {m.value}
            </span>
          </div>
        ))}
      </div>
      <p className="text-xs font-mono text-muted-foreground uppercase tracking-wider mb-2">
        컬럼 목록
      </p>
      <div className="flex flex-wrap gap-1.5">
        {metadata.prepare_summary.text_columns?.map((col: any) => (
          <span
            key={col}
            className="px-2.5 py-1 rounded-md text-[10px] font-mono font-medium bg-muted border border-border text-foreground"
          >
            {col}
          </span>
        )) || `-`}
      </div>
    </div>
  );
}
