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
import { Loader2, Play, RotateCw } from "lucide-react";
import { useParams } from "react-router-dom";
import { cn } from "@/lib/utils";
import { compactObject } from "@/shared/utils/clean";
import { useBuildJob } from "../hooks/build.mutation";
import BuildCleanForm from "./forms/BuildCleanForm";
import BuildGenuinenessForm from "./forms/BuildGenuinenessForm";
import type {
  BuildClauseFormValues,
  BuildCleanFormValues,
  BuildGenuinenessFormValues,
} from "../schemas/build.schema";
import { BuildClauseForm } from "./forms/BuildClauseForm";
import type { BuildJobType } from "@/shared/types/common";

export default function BuildDialog({
  formId,
  stage,
  status,
}: {
  stage: BuildJobType;
  formId: string;
  status: string;
}) {
  const { projectId, datasetId } = useParams();
  const { mutateAsync } = useBuildJob();
  const [open, setOpen] = useState(false);

  const close = () => setOpen(false);
  const isRunning = status === "queued";

  if (!projectId || !datasetId) return null;
  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          variant={status === "ready" ? "secondary" : "outline"}
          disabled={isRunning}
          className={cn(
            "flex-1 h-7 gap-1.5 rounded-lg px-2.5 text-xs transition-all",
            isRunning && "cursor-not-allowed opacity-70",
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
          <DialogTitle>분석을 실행하시겠습니까?</DialogTitle>
        </DialogHeader>
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
                await mutateAsync({
                  type: "doc_genuineness",
                  req: compactObject({
                    doc_genuineness_prompt_version: data.promptVersion,
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
                await mutateAsync({
                  type: "clause_label",
                  req: compactObject({
                    clause_label_prompt_version: data.promptVersion,
                    include_genuineness: data.includeGenuineness,
                  }),
                });
              }}
              onSuccess={close}
            />
          )}
        </div>
        <DialogFooter className="flex gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            취소
          </Button>
          <Button type="submit" form={formId}>
            실행
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
