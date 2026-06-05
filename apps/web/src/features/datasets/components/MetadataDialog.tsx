import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { AlertCircle, Pencil } from "lucide-react";
import { useState } from "react";
import type { DatasetMeta } from "../schemas/dataset";
import DatasetMetaForm from "./forms/DatasetMetaForm";
import { useEditMetadata } from "../hooks/dataset.mutation";

export default function MetadataDialog({ datasetId }: { datasetId: string }) {
  const { mutateAsync } = useEditMetadata();

  const [open, setOpen] = useState(false);
  const close = () => setOpen(false);

  const handleSubmit = async (data: DatasetMeta) => {
    await mutateAsync({ req: data, datasetId }).then(() => close());
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          variant="ghost"
          className="hover:bg-blue-50 hover:text-blue-500 text-zinc-400"
        >
          <Pencil className="w-3.5 h-3.5" />
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader className="shrink-0">
          <DialogTitle>분석 메타데이터 설정</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          <div>
            <div className="bg-amber-50 my-3 border border-amber-200 rounded-lg p-2 flex gap-3">
              <AlertCircle className="w-3 h-3 text-amber-500 shrink-0 mt-0.5" />
              <p className="text-[11px] text-amber-500">
                등록한 정보는 문서 진성 분석 실행 시 Prompt 변수로 자동
                활용됩니다.
              </p>
            </div>
            <DatasetMetaForm onSubmit={handleSubmit} />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={close}>
            취소
          </Button>
          <Button type="submit" form="dataset-meta-form">
            수정
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
