import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Loader2, Play, RotateCw, AlertTriangle, RefreshCw } from "lucide-react";
import { useParams } from "react-router-dom";
import { cn } from "@/lib/utils";
import { compactObject } from "@/shared/utils/clean";
import { useBuildJob } from "../hooks/build.mutation";
import { useLloaModelOptions } from "../hooks/build.query";
import BuildCleanForm from "./forms/BuildCleanForm";
import BuildGenuinenessForm from "./forms/BuildGenuinenessForm";
import type {
  BuildClauseFormValues,
  BuildCleanFormValues,
  BuildGenuinenessFormValues,
  BuildKeywordFormValues,
} from "../schemas/build.schema";
import { BuildClauseForm } from "./forms/BuildClauseForm";
import type { BuildJobType } from "@/shared/types/common";
import BuildKeywordForm from "./forms/BuildKeywordForm";

export default function BuildDialog({
  formId,
  stage,
  status,
  disabled = false,
  prereqReady = true,
  prereqLabel = "",
}: {
  stage: BuildJobType;
  formId: string;
  status: string;
  /** 외부 사유(예: 다운로드 중)로 강제 비활성화 */
  disabled?: boolean;
  /** 선행 단계가 완료됐는지. false면 실행 대신 경고를 띄운다. */
  prereqReady?: boolean;
  /** 선행 단계 이름(경고 문구용). */
  prereqLabel?: string;
}) {
  const { projectId, datasetId } = useParams();
  const { mutateAsync } = useBuildJob();
  const { data: lloaModels = [] } = useLloaModelOptions();
  const [open, setOpen] = useState(false);

  const close = () => setOpen(false);
  const isRunning = status === "queued" || status === "running";
  const blocked = isRunning || disabled;

  if (!projectId || !datasetId) return null;
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          variant={status === "ready" ? "secondary" : "outline"}
          disabled={blocked}
          className={cn(
            "flex-1 h-7 gap-1.5 rounded-lg px-2.5 text-xs transition-all",
            blocked && "cursor-not-allowed opacity-70",
            status === "ready" && "text-blue-600 hover:text-blue-700",
          )}
        >
          {isRunning ? (
            <>
              <Loader2 className="w-3.5 h-3.5 animate-spin" />
              실행 중
            </>
          ) : status === "ready" ? (
            <>
              <RotateCw className="w-3.5 h-3.5" />
              재실행
            </>
          ) : (
            <>
              <Play className="w-3.5 h-3.5" />
              실행
            </>
          )}
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md flex flex-col max-h-[80vh]">
        <DialogHeader className="shrink-0">
          <DialogTitle>
            {prereqReady ? "분석을 실행하시겠습니까?" : "이전 단계를 먼저 완료하세요"}
          </DialogTitle>
        </DialogHeader>
        {!prereqReady ? (
          <div className="flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
            <p>
              <b>{prereqLabel}</b> 단계가 아직 완료되지 않았습니다. 파이프라인은 순서대로
              실행해야 합니다 — 먼저 {prereqLabel}를 완료한 뒤 이 단계를 실행하세요.
            </p>
          </div>
        ) : (
        <div className="flex-1 overflow-y-auto">
          {stage == "clean" && (
            <BuildCleanForm
              formId={formId}
              onSubmit={async (data: BuildCleanFormValues) => {
                await mutateAsync({
                  type: "clean",
                  req: {
                    text_columns:
                      data.textColumns?.map((item) => item.value) ?? [],
                    date_column: data.dateColumn?.trim()
                      ? data.dateColumn.trim()
                      : null,
                  },
                });
              }}
              onSuccess={close}
            />
          )}
          {stage == "doc_genuineness" && (
            <BuildGenuinenessForm
              formId={formId}
              onSubmit={async (data: BuildGenuinenessFormValues) => {
                // 교차검증 모드: allowlist 앞 두 모델을 classify, 두 번째를 judge로(고정 preset).
                if (data.verify && lloaModels.length >= 2) {
                  await mutateAsync({
                    type: "doc_genuineness",
                    req: compactObject({
                      doc_genuineness_prompt_version: data.promptVersion,
                      verify: true,
                      classify_models: [lloaModels[0].model_id, lloaModels[1].model_id],
                      judge_model: lloaModels[1].model_id,
                    }),
                  });
                  return;
                }
                await mutateAsync({
                  type: "doc_genuineness",
                  req: compactObject({
                    doc_genuineness_prompt_version: data.promptVersion,
                    model_id: data.modelId,
                  }),
                });
              }}
              onSuccess={close}
            />
          )}
          {stage == "clause_label" && (
            <BuildClauseForm
              formId={formId}
              onSubmit={async (data: BuildClauseFormValues) => {
                // 교차검증 모드(ADR-028): allowlist 앞 두 모델을 classify, 두 번째를 judge로(고정 preset).
                if (data.verify && lloaModels.length >= 2) {
                  await mutateAsync({
                    type: "clause_label",
                    req: compactObject({
                      clause_label_prompt_version: data.promptVersion,
                      include_genuineness: data.includeGenuineness,
                      verify: true,
                      classify_models: [lloaModels[0].model_id, lloaModels[1].model_id],
                      judge_model: lloaModels[1].model_id,
                    }),
                  });
                  return;
                }
                await mutateAsync({
                  type: "clause_label",
                  req: compactObject({
                    clause_label_prompt_version: data.promptVersion,
                    include_genuineness: data.includeGenuineness,
                    model_id: data.modelId,
                  }),
                });
              }}
              onSuccess={close}
            />
          )}
          {stage == "clause_keywords" && (
            <BuildKeywordForm
              formId={formId}
              onSubmit={async (data: BuildKeywordFormValues) => {
                await mutateAsync({
                  type: "clause_keywords",
                  req: compactObject({
                    keyword_min_len: data.keywordMinLen,
                  }),
                });
              }}
              onSuccess={close}
            />
          )}
        </div>
        )}
        {/* 실행 후 보고서 반영 안내 — 결과는 자동 갱신되지만 보고서는 새로고침이 필요. */}
        {prereqReady && (
          <div className="flex items-start gap-1.5 rounded-lg bg-blue-50 px-3 py-2 text-[12px] font-medium text-blue-700">
            <RefreshCw className="mt-px h-3.5 w-3.5 shrink-0" strokeWidth={2.2} />
            <span>완료 후, 보고서 업데이트를 위해 새로고침을 진행해주세요.</span>
          </div>
        )}
        <DialogFooter className="flex gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            {prereqReady ? "취소" : "확인"}
          </Button>
          {prereqReady && (
            <Button type="submit" form={formId}>
              실행
            </Button>
          )}
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
