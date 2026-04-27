import {
  Field,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { zodResolver } from "@hookform/resolvers/zod";
import { Controller, useForm } from "react-hook-form";
import {
  versionSchema,
  type UploadVersionFormValues,
} from "../../schcema/dataset.schcema";
import { FileText, Upload, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { useState } from "react";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

interface UploadVersionFormProps {
  formId: string,
  type:"structured" | "unstructured"
  onSubmit: (data: UploadVersionFormValues) => Promise<void>
  onSuccess: () => void
}

const ANALYSIS_OPTIONS: { value: any; label: string; desc: string }[] = [
  { value: "sentiment", label: "감성 분석", desc: "텍스트 긍/부정 분류" },
  { value: "prepare", label: "전처리", desc: "정제 및 정규화" },
  { value: "embedding", label: "임베딩", desc: "벡터 변환" },
];

export default function UploadVersionForm({
  formId,
  type,
  onSubmit,
  onSuccess,
}: UploadVersionFormProps) {
  const [isDragging, setIsDragging] = useState(false);
  const {
    handleSubmit,
    control,
    watch,
    setValue,
    formState: { errors },
  } = useForm<UploadVersionFormValues>({
    resolver: zodResolver(versionSchema),
  });

  const file = watch("file");
  const analysisType = watch("analysisType");

  async function handleFormSubmit(data: UploadVersionFormValues) {
    await onSubmit(data)
    onSuccess()
  }

  function handleFile(f: File) {
    setValue("file", f, { shouldValidate: true });
  }

  function handleDrop(e: React.DragEvent) {
    e.preventDefault();
    setIsDragging(false);
    const f = e.dataTransfer.files[0];
    if (f) handleFile(f);
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-6 py-5">
        <Field>
          <FieldLabel className="text-xs">
            파일 <span className="text-red-500">*</span>
          </FieldLabel>

          {file ? (
            <div className="flex items-center gap-3 px-4 py-3 border border-indigo-200 bg-indigo-50 rounded-xl">
              <div className="w-8 h-8 rounded-lg bg-indigo-100 flex items-center justify-center shrink-0">
                <FileText className="w-4 h-4 text-indigo-500" />
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-zinc-800 truncate">
                  {file.name}
                </p>
                <p className="text-[11px] text-zinc-400">
                  {(file.size / 1024 / 1024).toFixed(2)} MB
                </p>
              </div>
              <button
                type="button"
                onClick={() =>
                  setValue("file", undefined as any, { shouldValidate: true })
                }
                className="w-6 h-6 flex items-center justify-center rounded-md text-zinc-400 hover:text-zinc-600 hover:bg-zinc-100 transition-colors"
              >
                <X className="w-3.5 h-3.5" />
              </button>
            </div>
          ) : (
            <label
              className={cn(
                "flex flex-col items-center gap-2 py-7 border-[1.5px] border-dashed rounded-xl cursor-pointer transition-colors",
                isDragging
                  ? "border-indigo-400 bg-indigo-50"
                  : "border-zinc-200 hover:border-indigo-300 hover:bg-zinc-50",
              )}
              onDragOver={(e) => {
                e.preventDefault();
                setIsDragging(true);
              }}
              onDragLeave={() => setIsDragging(false)}
              onDrop={handleDrop}
            >
              <div
                className={cn(
                  "w-9 h-9 rounded-xl flex items-center justify-center transition-colors",
                  isDragging ? "bg-indigo-100" : "bg-zinc-100",
                )}
              >
                <Upload
                  className={cn(
                    "w-4 h-4",
                    isDragging ? "text-indigo-500" : "text-zinc-400",
                  )}
                />
              </div>
              <div className="text-center">
                <p className="text-sm text-zinc-600">클릭하거나 드래그하세요</p>
                <p className="text-[11px] text-zinc-400 mt-0.5">
                  .csv, .json, .xlsx · 최대 100MB
                </p>
              </div>
              <input
                type="file"
                className="hidden"
                accept=".csv,.json,.xlsx,.txt"
                onChange={(e) => {
                  const f = e.target.files?.[0];
                  if (f) handleFile(f);
                }}
              />
            </label>
          )}

          {errors.file && (
            <p className="text-xs text-red-500">{errors.file.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            데이터 타입 <span className="text-red-500">*</span>
          </FieldLabel>
          <Controller
            control={control}
            name="dataType"
            defaultValue={type}
            render={({ field }) => (
              <RadioGroup
                value={field.value}
                onValueChange={(v) =>
                  field.onChange(v as "structured" | "unstructured")
                }
                className="flex"
              >
                <FieldLabel>
                  <Field orientation="horizontal">
                    <RadioGroupItem value="structured" />
                    <FieldTitle>정형</FieldTitle>
                  </Field>
                </FieldLabel>
                <FieldLabel>
                  <Field orientation="horizontal">
                    <RadioGroupItem value="unstructured" />
                    <FieldTitle>비정형</FieldTitle>
                  </Field>
                </FieldLabel>
              </RadioGroup>
            )}
          />
          {errors.dataType && (
            <p className="text-xs text-red-500">{errors.dataType.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            분석 유형 <span className="text-red-500">*</span>
          </FieldLabel>

          <div className="grid grid-cols-3 gap-2">
            {ANALYSIS_OPTIONS.map((opt) => (
              <button
                key={opt.value}
                type="button"
                onClick={() =>
                  setValue("analysisType", opt.value, { shouldValidate: true })
                }
                className={cn(
                  "flex flex-col items-center gap-1 py-3 px-2 border rounded-xl text-center transition-colors",
                  analysisType === opt.value
                    ? "border-indigo-400 bg-indigo-50 text-indigo-700"
                    : "border-zinc-200 hover:border-zinc-300 hover:bg-zinc-50 text-zinc-600",
                )}
              >
                <span className="text-xs font-medium">{opt.label}</span>
                <span className="text-[10px] text-zinc-400">{opt.desc}</span>
              </button>
            ))}
          </div>

          {errors.analysisType && (
            <p className="text-xs text-red-500">
              {errors.analysisType.message}
            </p>
          )}
        </Field>
      </FieldGroup>
    </form>
  );
}
