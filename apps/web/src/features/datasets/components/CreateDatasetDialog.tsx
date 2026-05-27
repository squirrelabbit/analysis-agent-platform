import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { AlertCircle, CheckCircle2, Plus } from "lucide-react";
import { useState } from "react";
import type { DatasetInfo, DatasetMeta } from "../schemas/dataset";
import DatasetInfoForm from "./forms/DatasetInfoForm";
import DatasetMetaForm from "./forms/DatasetMetaForm";
import { useCreateDataset } from "../hooks/dataset.mutation";

export default function CreateDatasetDialog() {
  const { mutateAsync } = useCreateDataset();
  const [currentStep, setCurrentStep] = useState<number>(1);
  const [datasetInfo, setDatasetInfo] = useState<DatasetInfo | null>(null);

  const [open, setOpen] = useState(false);
  const close = () => setOpen(false);

  const handlePrevious = () => {
    if (currentStep === 2) {
      setCurrentStep(1);
    }
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
    }).then(() => close());
  };

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button className="text-xs">
          <Plus className="w-3.5 h-3.5" />
          데이터셋 생성
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader className="shrink-0">
          <DialogTitle>데이터셋 생성</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          <>
            {/* 진행 단계 표시 */}
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
                      {step === 2 && "진정 분석 설정"}
                    </p>
                  </div>
                </div>
              ))}
            </div>
            {/* Step 1: 기본 정보 */}
            {currentStep === 1 && (
              <DatasetInfoForm
                onSubmit={handleInfoSubmit}
                datasetInfo={datasetInfo}
              />
            )}

            {/* Step 2: 진정 분석 설정 */}
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
          </>
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
