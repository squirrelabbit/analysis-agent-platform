import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { zodResolver } from "@hookform/resolvers/zod";
import { Controller, useFieldArray, useForm } from "react-hook-form";
import {
  versionSchema,
  type UploadVersionFormValues,
} from "../../../schcema/dataset.schcema";
import { Plus, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Input } from "@/components/ui/input";
import FileUploader from "@/components/common/files/FileUploader";
import { CleanOptionsAccordion } from "./CleanOptionsAccordion";
import { Button } from "@/components/ui/button";

interface UploadVersionFormProps {
  formId: string;
  type: "structured" | "unstructured";
  onSubmit: (data: UploadVersionFormValues) => Promise<void>;
  onSuccess: () => void;
}

// const ANALYSIS_OPTIONS: { value: any; label: string; desc: string }[] = [
//   { value: "sentiment", label: "감성 분석", desc: "텍스트 긍/부정 분류" },
//   { value: "prepare", label: "전처리", desc: "정제 및 정규화" },
//   { value: "embedding", label: "임베딩", desc: "벡터 변환" },
// ];

const TYPE_OPTIONS = [
  { value: "structured", label: "정형", desc: "CSV, 테이블 형식" },
  { value: "unstructured", label: "비정형", desc: "텍스트, JSON 자유형" },
];

export default function UploadVersionForm({
  formId,
  type,
  onSubmit,
  onSuccess,
}: UploadVersionFormProps) {
  const {
    register,
    handleSubmit,
    control,
    // watch,
    formState: { errors },
  } = useForm<UploadVersionFormValues>({
    resolver: zodResolver(versionSchema),
    defaultValues: {
      dataType: type,
      text_columns: [{ value: "" }],
      analysisType: "prepare",
      cleanOptions: {
        remove_english: false,
        remove_numbers: false,
        remove_special: false,
        remove_monosyllables: false,
      },
    },
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: "text_columns",
  });

  // const analysisType = watch("analysisType");

  async function handleFormSubmit(data: UploadVersionFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <Field>
          <FieldLabel className="text-xs">
            파일 <span className="text-red-500">*</span>
          </FieldLabel>
          <Controller
            name="file"
            control={control}
            render={({ field }) => (
              <FileUploader value={field.value} onChange={field.onChange} />
            )}
          />
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
                {TYPE_OPTIONS.map((opt) => (
                  <FieldLabel key={opt.value}>
                    <Field orientation="horizontal">
                      <RadioGroupItem value={opt.value} />
                      <div>
                        <FieldTitle className="text-xs">{opt.label}</FieldTitle>
                        <FieldDescription className="text-[10px] text-zinc-400">
                          {opt.desc}
                        </FieldDescription>
                      </div>
                    </Field>
                  </FieldLabel>
                ))}
              </RadioGroup>
            )}
          />
          {errors.dataType && (
            <p className="text-xs text-red-500">{errors.dataType.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">전처리 옵션<p className="text-xs text-zinc-300">(선택)</p></FieldLabel>
          <Controller
            control={control}
            name="cleanOptions"
            render={({ field }) => (
              <CleanOptionsAccordion
                value={field.value}
                onChange={field.onChange}
              />
            )}
          />
        </Field>
        <Field>
          <div className="flex items-center justify-between mb-1">
            <FieldLabel className="text-xs">
              불용어 키워드 <span className="text-red-500">*</span>
            </FieldLabel>
          </div>
          <div className="flex flex-col gap-2">
            {fields.map((field, idx) => (
              <div key={field.id} className="flex items-center gap-2">
                <Input
                  {...register(`text_columns.${idx}.value` as const)}
                  placeholder="예: 광고"
                />
                <Button
                  variant="outline"
                  onClick={() => fields.length > 1 && remove(idx)}
                  disabled={fields.length <= 1}
                  className={cn(
                    "w-7 h-7 flex items-center justify-center rounded-lg border transition-colors shrink-0",
                    fields.length > 1
                      ? "border-zinc-200 text-zinc-400 hover:bg-red-50 hover:text-red-400 hover:border-red-200"
                      : "border-zinc-100 text-zinc-200 cursor-not-allowed",
                  )}
                >
                  <X className="w-3 h-3" />
                </Button>
              </div>
            ))}

            {errors.text_columns && (
              <p className="text-xs text-red-500">
                {errors.text_columns.message ??
                  "키워드 정보를 올바르게 입력하세요"}
              </p>
            )}

            <Button
              variant="outline"
              onClick={() => append({ value: "" })}
              className="flex items-center justify-center gap-1.5 w-full py-2 border border-dashed border-zinc-200 rounded-xl text-xs text-zinc-400 hover:border-indigo-300 hover:text-indigo-500 hover:bg-indigo-50 transition-colors"
            >
              <Plus className="w-3.5 h-3.5" />
              키워드 추가
            </Button>
          </div>
        </Field>
        {/* <Field>
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
        </Field> */}
      </FieldGroup>
    </form>
  );
}
