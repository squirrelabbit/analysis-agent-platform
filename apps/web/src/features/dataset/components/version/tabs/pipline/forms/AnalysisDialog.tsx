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
import CleanJobForm from "./CleanJobForm";
import type { Stage } from "@/features/dataset/types/datasetVersion";
import { useParams } from "react-router-dom";
import { useBuildJob } from "@/features/dataset/hooks/version.mutation";
import { cn } from "@/lib/utils";
import { mapCleanJobFormToRequest } from "@/features/dataset/api/datasetVersion.mapper";
import DocGenuinenessJobForm from "./DocGenuinenessJobForm";
import { compactObject } from "@/utils/clean";
import { ClauseLabelJobForm } from "./ClauseLabelJobForm";

export default function AnalysisDialog({
  versionId,
  formId,
  stage,
  status
}: {
  versionId: string,
  stage: Stage
  formId: string;
  status: string;
}) {
  const {projectId, datasetId } =useParams()
  const build = useBuildJob()
  const [open, setOpen] = useState(false);

  const close = () => setOpen(false)
  const isRunning = status === "queued"
  
  if(!projectId || !datasetId) return null
  return (
    <Dialog open={open} onOpenChange={setOpen} >
      <DialogTrigger asChild>
        <Button
          variant={status === "ready" ? "secondary" : "outline"}
          disabled={isRunning}
          className={cn(
            "h-7 gap-1.5 rounded-lg px-2.5 text-xs transition-all",
            isRunning &&
              "cursor-not-allowed opacity-70",
            status === "ready" &&
              "text-blue-600 hover:text-blue-700"
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
          {stage == 'clean' && <CleanJobForm
            formId={formId}
            onSubmit={async (data) => {
              await build.mutateAsync({projectId: projectId, datasetId: datasetId, versionId:  versionId,stage: stage, req: mapCleanJobFormToRequest(data)})
            }}
            onSuccess={close}
          />}
          {stage == 'docGenuineness' && <DocGenuinenessJobForm
            formId={formId}
            onSubmit={async (data) => {
              await build.mutateAsync({projectId: projectId, datasetId: datasetId, versionId:  versionId,stage: stage, req: compactObject({doc_genuineness_prompt_version: data.promptVersion})})
            }}
            onSuccess={close}
          />}
          {stage == 'clauseLabel' && <ClauseLabelJobForm
            formId={formId}
            onSubmit={async () => {
              await build.mutateAsync({projectId: projectId, datasetId: datasetId, versionId:  versionId,stage: stage})
            }}
            onSuccess={close}
          />}
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
