import { Controller, useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import z from "zod";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { KeywordInput } from "../components/KeywordInput";
import type { Dataset } from "../models/model";
import { useEditDatasetInfo, useEditMetadata } from "../hooks/dataset.mutation";
import { useTaxonomies } from "@/features/taxonomy/hooks/taxonomy.query";

// Radix Select은 빈 문자열 value를 허용하지 않아 "기본값" 항목용 sentinel을 둔다.
const TAXONOMY_DEFAULT = "__default__";

// 통합 수정 폼: 이름/설명 + aspect taxonomy + 진성 분석 설정(metadata). 이름만 필수, 나머지는 선택.
const editSchema = z.object({
  name: z.string().trim().min(1, "이름은 필수입니다"),
  description: z.string(),
  taxonomyId: z.string().optional(),
  subjectType: z.string(),
  subjectName: z.string(),
  subjectAliases: z.array(z.string()),
  recruitmentKeywords: z.array(z.string()),
});
type EditValues = z.infer<typeof editSchema>;

/*
 * 데이터셋 수정 (이름/설명 + 진성 분석 설정). silverone 2026-06-05.
 * 저장 시 PATCH /datasets/{id}(이름/설명) + PATCH /datasets/{id}/metadata(doc_genuineness)
 * 두 호출. Radix Dialog는 닫히면 unmount → 열 때마다 현재값으로 prefill(effect 불필요).
 */
export default function EditInfoDialogControlled({
  dataset,
  open,
  onOpenChange,
}: {
  dataset: Dataset;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const { mutateAsync: editInfo } = useEditDatasetInfo();
  const { mutateAsync: editMeta } = useEditMetadata();
  const { data: taxonomies } = useTaxonomies();

  const dg = dataset.docGenuineness;
  const {
    register,
    handleSubmit,
    control,
    formState: { errors, isSubmitting },
  } = useForm<EditValues>({
    resolver: zodResolver(editSchema),
    defaultValues: {
      name: dataset.name,
      description: dataset.description ?? "",
      taxonomyId: dg?.taxonomyId ?? "",
      subjectType: dg?.subjectType ?? "",
      subjectName: dg?.subjectName ?? "",
      subjectAliases: dg?.subjectAliases ?? [],
      recruitmentKeywords: dg?.recruitmentKeywords ?? [],
    },
  });

  const onSubmit = async (data: EditValues) => {
    await editInfo({
      datasetId: dataset.id,
      name: data.name.trim(),
      description: data.description,
    });
    await editMeta({
      datasetId: dataset.id,
      req: {
        taxonomyId: data.taxonomyId,
        subjectType: data.subjectType,
        subjectName: data.subjectName,
        subjectAliases: data.subjectAliases,
        recruitmentKeywords: data.recruitmentKeywords,
      },
    });
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg flex flex-col max-h-[85vh]">
        <DialogHeader className="shrink-0">
          <DialogTitle>데이터셋 수정</DialogTitle>
        </DialogHeader>
        <form
          id="dataset-edit-form"
          onSubmit={handleSubmit(onSubmit)}
          className="flex-1 overflow-y-auto"
        >
          <div className="flex flex-col gap-4 py-1">
            {/* 기본 정보 */}
            <Field>
              <FieldLabel className="text-xs">
                이름 <span className="text-red-500">*</span>
              </FieldLabel>
              <Input {...register("name")} placeholder="데이터셋 이름" />
              {errors.name && (
                <p className="text-xs text-red-500">{errors.name.message}</p>
              )}
            </Field>
            <Field>
              <FieldLabel className="text-xs">설명</FieldLabel>
              <Textarea
                {...register("description")}
                placeholder="데이터셋 설명"
                rows={2}
              />
            </Field>

            {/* aspect taxonomy (per-dataset). 절 라벨링/분석이 이 aspect 체계를 쓴다. */}
            <Controller
              control={control}
              name="taxonomyId"
              render={({ field }) => (
                <Field>
                  <FieldLabel className="text-xs">
                    Aspect 분류 체계 (taxonomy)
                  </FieldLabel>
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

            {/* 진성 분석 설정 */}
            <div className="mt-1 rounded-lg border border-slate-200 bg-slate-50 p-4 flex flex-col gap-4">
              <p className="text-xs font-medium text-slate-600">
                진성 분석 설정 (문서 진성 분석 실행 시 Prompt 변수로 활용)
              </p>
              <div className="grid grid-cols-2 gap-3">
                <Field>
                  <FieldLabel className="text-xs">대상 유형</FieldLabel>
                  <Input
                    {...register("subjectType")}
                    placeholder="예) festival"
                  />
                </Field>
                <Field>
                  <FieldLabel className="text-xs">대상명</FieldLabel>
                  <Input
                    {...register("subjectName")}
                    placeholder="예) 강릉 국가유산야행"
                  />
                </Field>
              </div>
              <Controller
                control={control}
                name="subjectAliases"
                render={({ field }) => (
                  <Field>
                    <FieldLabel className="text-xs">연관 키워드 (별칭)</FieldLabel>
                    <KeywordInput
                      value={field.value}
                      onChange={field.onChange}
                      placeholder="엔터로 추가"
                    />
                    <FieldDescription className="text-xs">
                      분석 대상과 관련된 키워드
                    </FieldDescription>
                  </Field>
                )}
              />
              <Controller
                control={control}
                name="recruitmentKeywords"
                render={({ field }) => (
                  <Field>
                    <FieldLabel className="text-xs">
                      모집 키워드 (포함 시 진정 제외)
                    </FieldLabel>
                    <KeywordInput
                      value={field.value}
                      onChange={field.onChange}
                      placeholder="엔터로 추가"
                    />
                    <FieldDescription className="text-xs">
                      이 키워드가 포함되면 분석에서 제외
                    </FieldDescription>
                  </Field>
                )}
              />
            </div>
          </div>
        </form>
        <DialogFooter className="flex gap-2 shrink-0">
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            취소
          </Button>
          <Button type="submit" form="dataset-edit-form" disabled={isSubmitting}>
            저장
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
