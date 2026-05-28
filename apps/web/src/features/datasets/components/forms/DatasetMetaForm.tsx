import { Controller, useForm } from "react-hook-form";
import { datasetMetaSchema, type DatasetMeta } from "../../schemas/dataset";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { KeywordInput } from "../KeywordInput";

export default function DatasetMetaForm({
  onSubmit,
}: {
  onSubmit: (data: DatasetMeta) => void;
}) {
  const {
    register,
    handleSubmit,
    control,
    formState: { errors },
  } = useForm<DatasetMeta>({
    resolver: zodResolver(datasetMetaSchema),
    defaultValues: {
      subjectType: "",
      subjectName: "",
      subjectAliases: [],
      recruitmentKeywords: [],
    },
  });

  return (
    <form id={`dataset-meta-form`} onSubmit={handleSubmit(onSubmit)}>
      {/* 분석 대상 유형 + 이름 */}
      <div className="grid grid-cols-2 gap-4 space-y-2">
        <Field>
          <FieldLabel className="text-xs">
            대상 유형 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input {...register("subjectType")} placeholder="예) festival" />
          {errors.subjectType && (
            <p className="text-xs text-red-500">{errors.subjectType.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            대상명 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input
            {...register("subjectName")}
            placeholder="예) 강릉 국가유산야행"
          />
          {errors.subjectName && (
            <p className="text-xs text-red-500">{errors.subjectName.message}</p>
          )}
        </Field>
      </div>

      {/* 키워드 설정 */}
      <div className="space-y-4 bg-slate-50 rounded-lg p-4 border border-slate-200">

        {/* 관련 키워드 */}
        <Controller
          control={control}
          name="subjectAliases"
          render={({ field }) => (
            <Field>
              <div className="flex items-center gap-2 mb-2">
                <div className="w-3 h-3 rounded-full bg-blue-500" />
                <FieldLabel className="text-sm font-medium text-slate-900">
                  연관 키워드 (별칭·즐임말)
                </FieldLabel>
              </div>
              <KeywordInput
                value={field.value}
                onChange={field.onChange}
                placeholder="엔터로 추가"
              />
              <FieldDescription className="text-xs">
                분석 대상과 관련된 키워드를 입력하세요
              </FieldDescription>
              {errors.subjectAliases && (
                <p className="text-xs text-red-500">
                  {errors.subjectAliases.message}
                </p>
              )}
            </Field>
          )}
        />

        {/* 제외 키워드 */}
        <Controller
          control={control}
          name="recruitmentKeywords"
          render={({ field }) => (
            <Field>
              <div className="flex items-center gap-2 mb-2">
                <div className="w-3 h-3 rounded-full bg-amber-500" />
                <FieldLabel className="text-sm font-medium text-slate-900">
                  모질 키워드 (포함 시 진정 제외)
                </FieldLabel>
              </div>
              <KeywordInput
                value={field.value || []}
                onChange={field.onChange}
                placeholder="엔터로 추가"
              />
              <FieldDescription className="text-xs">
                이 키워드가 포함되면 분석에서 제외합니다
              </FieldDescription>
              {errors.recruitmentKeywords && (
                <p className="text-xs text-red-500">
                  {errors.recruitmentKeywords.message}
                </p>
              )}
            </Field>
          )}
        />
      </div>
    </form>
  );
}
