import type { FormProps } from "@/shared/models/common";
import { BuildGenuinenessSchema, type BuildGenuinenessFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { FieldGroup } from "@/components/ui/field";
import { Checkbox } from "@/components/ui/checkbox";
import PromptVersionField from "@/features/prompts/components/PromptVersionField";
import LloaModelField from "./LloaModelField";
import { useLloaModelOptions } from "../../hooks/build.query";

export default function BuildGenuinenessForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildGenuinenessFormValues>) {
  const {
    handleSubmit,
    setValue,
    control,
    formState: { errors },
  } = useForm<BuildGenuinenessFormValues>({
    resolver: zodResolver(BuildGenuinenessSchema),
  });

  const promptVersion = useWatch({ control, name: "promptVersion" }) ?? "";
  const modelId = useWatch({ control, name: "modelId" }) ?? "";
  const verify = useWatch({ control, name: "verify" }) ?? false;

  // 교차검증 preset — allowlist 앞의 두 모델을 classify, 두 번째를 judge로(고정 preset).
  const { data: models = [] } = useLloaModelOptions();
  const canVerify = models.length >= 2;
  const presetA = models[0];
  const presetB = models[1];

  async function handleFormSubmit(data: BuildGenuinenessFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <p className="text-sm text-zinc-500">
          문서별로 실제 리뷰 여부(진성)를 분류합니다. 데이터 정제 완료 후
          실행할 수 있습니다.
        </p>
        <PromptVersionField
          task="doc_genuineness"
          value={promptVersion}
          onChange={(v) =>
            setValue("promptVersion", v, { shouldValidate: true })
          }
          errorMessage={errors.promptVersion?.message}
        />

        {/* 교차검증 모드 토글 (ADR-026) */}
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
                  두 모델로 교차 분류 + 불일치 시 judge가 최종 라벨 결정
                </span>
              </span>
            </label>
            {verify && presetA && presetB && (
              <p className="mt-2 pl-6 text-[11px] leading-relaxed text-zinc-500">
                분류: <b>{presetA.label}</b> + <b>{presetB.label}</b> · judge:{" "}
                <b>{presetB.label}</b>
                <br />
                합의 문서는 그대로, 갈린 문서만 judge가 검토합니다(시간 더 걸림).
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
      </FieldGroup>
    </form>
  );
}
