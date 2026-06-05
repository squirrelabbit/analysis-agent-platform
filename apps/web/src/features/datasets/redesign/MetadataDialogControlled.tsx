import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { AlertCircle } from "lucide-react";
import type { DatasetMeta } from "../schemas/dataset";
import DatasetMetaForm from "../components/forms/DatasetMetaForm";
import { useEditMetadata } from "../hooks/dataset.mutation";

/*
 * 기존 MetadataDialog의 controlled 버전. 케밥 메뉴 "수정"(진정 분석 설정)에서 연다.
 * 폼/뮤테이션은 기존과 동일(DatasetMetaForm + useEditMetadata).
 */
export default function MetadataDialogControlled({
  datasetId,
  open,
  onOpenChange,
}: {
  datasetId: string;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { mutateAsync } = useEditMetadata();

  const handleSubmit = async (data: DatasetMeta) => {
    await mutateAsync({ req: data, datasetId }).then(() => onOpenChange(false));
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader className="shrink-0">
          <DialogTitle>진정 분석 설정</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          <div className="bg-amber-50 my-3 border border-amber-200 rounded-lg p-2 flex gap-3">
            <AlertCircle className="w-3 h-3 text-amber-500 shrink-0 mt-0.5" />
            <p className="text-[11px] text-amber-500">
              등록한 정보는 문서 진성 분석 실행 시 Prompt 변수로 자동 활용됩니다.
            </p>
          </div>
          <DatasetMetaForm onSubmit={handleSubmit} />
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
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
