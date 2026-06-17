import type { FormProps } from "@/shared/models/common";
import { BuildClauseSchema, type BuildClauseFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Checkbox } from "@/components/ui/checkbox";
import { AlertCircle } from "lucide-react";
import PromptVersionField from "@/features/prompts/components/PromptVersionField";
import LloaModelField from "./LloaModelField";
import { useLloaModelOptions } from "../../hooks/build.query";

// 2026-06-17 — 옛 "mixed" tier는 백엔드에서 제거되고 "uncertain"으로 통합됨
// (_ALLOWED_GENUINENESS_FILTER = genuine_review/non_review/uncertain). value를
// uncertain으로 맞추지 않으면 worker가 "invalid include_genuineness tier" 400.
const genuinenessOptions = [
  {
    label: "실제 리뷰",
    value: "genuine_review",
  },
  {
    label: "불확실(혼합)",
    value: "uncertain",
  },
];

export function BuildClauseForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildClauseFormValues>) {
  const {
    setValue,
    control,
    handleSubmit,
    formState: { errors },
  } = useForm<BuildClauseFormValues>({
    resolver: zodResolver(BuildClauseSchema),
  });

  const promptVersion = useWatch({ control, name: "promptVersion" }) ?? "";
  const currentInclude = useWatch({ control, name: "includeGenuineness" }) ?? [];
  const modelId = useWatch({ control, name: "modelId" }) ?? "";
  const verify = useWatch({ control, name: "verify" }) ?? false;

  // 교차검증 preset (ADR-028) — allowlist 앞 두 모델을 classify, 두 번째를 judge로.
  const { data: models = [] } = useLloaModelOptions();
  const canVerify = models.length >= 2;
  const presetA = models[0];
  const presetB = models[1];

  async function handleFormSubmit(data: BuildClauseFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <p className="text-sm text-zinc-500">
          문서를 절 단위로 나누고 각 절의 감성과 aspect(주제)를 라벨링합니다.
          진성 분석 완료 후 실행할 수 있습니다.
        </p>
        <PromptVersionField
          task="clause_label"
          value={promptVersion}
          onChange={(v) =>
            setValue("promptVersion", v, { shouldValidate: true })
          }
          errorMessage={errors.promptVersion?.message}
        />
        {/* 교차검증 모드 토글 (ADR-028) */}
        {canVerify && (
          <div className="rounded-lg border border-zinc-200 p-3">
            <label className="flex items-start gap-2">
              <Checkbox
                checked={verify}
                onCheckedChange={(c) => setValue("verify", c === true)}
                className="mt-0.5"
              />
              <span className="text-sm">
                <span className="font-medium text-zinc-700">교차검증 모드</span>
                <span className="ml-1 text-xs text-zinc-400">
                  두 모델이 같은 문장을 라벨링 + 갈린 절만 judge가 결정
                </span>
              </span>
            </label>
            {verify && presetA && presetB && (
              <p className="mt-2 pl-6 text-[11px] leading-relaxed text-zinc-500">
                분류: <b>{presetA.label}</b> + <b>{presetB.label}</b> · judge:{" "}
                <b>{presetB.label}</b>
                <br />
                합의 절은 그대로, 갈린 절만 judge가 검토합니다(시간 더 걸림).
              </p>
            )}
          </div>
        )}

        {/* 단일 모델 선택은 교차검증 OFF일 때만 */}
        {!verify && (
          <LloaModelField
            value={modelId}
            onChange={(v) => setValue("modelId", v, { shouldValidate: true })}
            errorMessage={errors.modelId?.message}
          />
        )}
        <Field>
          <FieldLabel className="text-xs">
            포함할 리뷰 유형
            <span className="ml-1 text-xs text-zinc-300">(선택)</span>
          </FieldLabel>

          <div className="space-y-2 rounded-lg border p-3">
            {genuinenessOptions.map((option) => (
              <label key={option.value} className="flex items-center gap-2">
                <Checkbox
                  checked={currentInclude.includes(option.value)}
                  onCheckedChange={(checked) => {
                    setValue(
                      "includeGenuineness",
                      checked
                        ? [...currentInclude, option.value]
                        : currentInclude.filter((v) => v !== option.value),
                    );
                  }}
                />

                <span className="text-sm">{option.label}</span>
              </label>
            ))}

            <div className="flex items-center gap-1 pt-1 text-[11px] text-amber-600">
              <AlertCircle className="h-3 w-3 shrink-0" />
              진성 분류 실행 후 사용 가능합니다
            </div>
          </div>
        </Field>
      </FieldGroup>
    </form>
  );
}
