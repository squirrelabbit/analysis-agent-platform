import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { AlertCircle, CheckCircle2 } from "lucide-react";
import { useState } from "react";
import type { DatasetInfo, DatasetMeta } from "../schemas/dataset";
import DatasetInfoForm from "../components/forms/DatasetInfoForm";
import DatasetMetaForm from "../components/forms/DatasetMetaForm";
import { useCreateDataset } from "../hooks/dataset.mutation";

/*
 * 기존 CreateDatasetDialog는 자체 트리거 버튼을 내장해 외부에서 못 연다.
 * 리디자인은 헤더 버튼 + 점선 add-card 두 곳에서 같은 생성 플로우를 열어야 해서
 * open/onOpenChange로 제어 가능한 버전을 둔다. 단계 오케스트레이션/폼/뮤테이션은 기존과 동일.
 */
export default function CreateDatasetDialogControlled({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { mutateAsync } = useCreateDataset();
  const [currentStep, setCurrentStep] = useState(1);
  const [datasetInfo, setDatasetInfo] = useState<DatasetInfo | null>(null);

  const reset = () => {
    setCurrentStep(1);
    setDatasetInfo(null);
  };

  // 닫힐 때 단계 초기화 (다시 열면 1단계부터). effect 대신 이벤트 핸들러에서 처리.
  const handleOpenChange = (next: boolean) => {
    if (!next) reset();
    onOpenChange(next);
  };

  const handlePrevious = () => {
    if (currentStep === 2) setCurrentStep(1);
  };

  const handleInfoSubmit = async (data: DatasetInfo) => {
    setDatasetInfo(data);
    setCurrentStep(2);
  };

  const handleMetaSubmit = async (data: DatasetMeta) => {
    if (!datasetInfo) return;
    await mutateAsync({
      ...datasetInfo,
      metadata: { docGenuineness: data },
    }).then(() => {
      reset();
      onOpenChange(false);
    });
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader className="shrink-0">
          <DialogTitle>데이터셋 생성</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          <div className="flex items-center justify-between px-6 py-4 bg-slate-50 rounded-lg">
            {[1, 2].map((step) => (
              <div key={step} className="flex items-center gap-3 flex-1">
                <div>
                  {step < currentStep ? (
                    <CheckCircle2 className="w-5 h-5" />
                  ) : (
                    step
                  )}
                </div>
                <div className="hidden sm:block">
                  <p className="text-xs font-medium text-slate-600">
                    {step === 1 && "기본 정보"}
                    {step === 2 && "분석 메타데이터 설정"}
                  </p>
                </div>
              </div>
            ))}
          </div>

          {currentStep === 1 && (
            <DatasetInfoForm
              onSubmit={handleInfoSubmit}
              datasetInfo={datasetInfo}
            />
          )}

          {currentStep === 2 && (
            <div>
              <div className="bg-amber-50 my-3 border border-amber-200 rounded-lg p-2 flex gap-3">
                <AlertCircle className="w-3 h-3 text-amber-500 shrink-0 mt-0.5" />
                <p className="text-[11px] text-amber-500">
                  등록한 정보는 문서 진성 분석 실행 시 Prompt 변수로 자동
                  활용됩니다.
                </p>
              </div>
              <DatasetMetaForm onSubmit={handleMetaSubmit} />
            </div>
          )}
        </div>
        <DialogFooter>
          {currentStep === 2 && (
            <Button variant="outline" onClick={handlePrevious}>
              이전
            </Button>
          )}
          <Button
            type="submit"
            form={currentStep === 1 ? "dataset-info-form" : "dataset-meta-form"}
          >
            {currentStep === 1 ? "다음" : "완료"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
