import { Controller, useForm } from "react-hook-form";
import { datasetMetaSchema, type DatasetMeta } from "../../schemas/dataset";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { KeywordInput } from "../KeywordInput";
import { useTaxonomies } from "@/features/taxonomy/hooks/taxonomy.query";

// Radix Select은 빈 문자열 value를 허용하지 않아 "기본값" 항목용 sentinel을 둔다.
const TAXONOMY_DEFAULT = "__default__";

export default function DatasetMetaForm({
  onSubmit,
}: {
  onSubmit: (data: DatasetMeta) => void;
}) {
  const { data: taxonomies } = useTaxonomies();
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
      taxonomyId: "",
      genuinenessExtraInstructions: "",
      genuinenessExtraExamples: "",
      clauseExtraInstructions: "",
      clauseExtraExamples: "",
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

      {/* aspect taxonomy (per-dataset). 절 라벨링/분석이 이 aspect 체계를 쓴다. */}
      <div className="mt-2">
        <Controller
          control={control}
          name="taxonomyId"
          render={({ field }) => (
            <Field>
              <FieldLabel className="text-xs">Aspect 분류 체계 (taxonomy)</FieldLabel>
              <Select
                value={field.value ? field.value : TAXONOMY_DEFAULT}
                onValueChange={(v) =>
                  field.onChange(v === TAXONOMY_DEFAULT ? "" : v)
                }
              >
                <SelectTrigger className="h-9 text-sm">
                  <SelectValue placeholder="기본값" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value={TAXONOMY_DEFAULT}>
                    기본값{taxonomies?.default ? ` (${taxonomies.default})` : ""}
                  </SelectItem>
                  {(taxonomies?.items ?? []).map((t) => (
                    <SelectItem key={t.taxonomy_id} value={t.taxonomy_id}>
                      {t.taxonomy_id}
                      {t.is_default ? " · 기본" : ""}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <FieldDescription className="text-xs">
                절 분리·감성·aspect 라벨링에 쓸 aspect 체계입니다. 미선택 시 기본값.
              </FieldDescription>
            </Field>
          )}
        />
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

      {/* 행사별 추가 지침/예시 (선택). festival base 프롬프트는 공통이고, 이 행사에만
          필요한 고유 규칙·예시만 여기에 적는다. 비우면 base 프롬프트만 사용한다.
          진성 분류(doc_genuineness)와 절 라벨링(clause_label)은 출력 형식이 달라 분리. */}
      <div className="mt-4 space-y-4 rounded-lg border border-slate-200 bg-slate-50 p-4">
        <div>
          <FieldLabel className="text-sm font-medium text-slate-900">
            행사별 추가 지침/예시 (선택)
          </FieldLabel>
          <FieldDescription className="text-xs">
            이 행사에만 해당하는 고유 규칙·예시만 적으세요. 비우면 기본 프롬프트만
            사용합니다. 입력 내용은 프롬프트에 그대로 들어가므로 운영자 검수 대상입니다.
          </FieldDescription>
        </div>

        {/* 진성 분류 (doc_genuineness) */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <div className="h-3 w-3 rounded-full bg-emerald-500" />
            <span className="text-xs font-semibold text-slate-700">진성 분류</span>
          </div>
          <Field>
            <FieldLabel className="text-xs">추가 지침</FieldLabel>
            <Textarea
              {...register("genuinenessExtraInstructions")}
              rows={2}
              placeholder="예) 입장료·특정 지명 언급은 현장 관찰 근거로 본다"
            />
          </Field>
          <Field>
            <FieldLabel className="text-xs">추가 예시</FieldLabel>
            <Textarea
              {...register("genuinenessExtraExamples")}
              rows={2}
              placeholder="문서 → genuine_review / non_review / uncertain 형식 예시"
            />
          </Field>
        </div>

        {/* 절 라벨링 (clause_label) */}
        <div className="space-y-2">
          <div className="flex items-center gap-2">
            <div className="h-3 w-3 rounded-full bg-violet-500" />
            <span className="text-xs font-semibold text-slate-700">절 라벨링</span>
          </div>
          <Field>
            <FieldLabel className="text-xs">추가 지침</FieldLabel>
            <Textarea
              {...register("clauseExtraInstructions")}
              rows={2}
              placeholder="예) 특정 프로그램명/장소명은 콘텐츠·대상으로 해석한다"
            />
          </Field>
          <Field>
            <FieldLabel className="text-xs">추가 예시</FieldLabel>
            <Textarea
              {...register("clauseExtraExamples")}
              rows={2}
              placeholder="문장 → {index, relevant, sentiment, aspects[]} 형식 예시"
            />
          </Field>
        </div>
      </div>
    </form>
  );
}
