import type { FormProps } from "@/shared/models/common";
import { BuildGenuinenessSchema, type BuildGenuinenessFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { FieldGroup } from "@/components/ui/field";
import PromptVersionField from "@/features/prompts/components/PromptVersionField";
import LloaModelField from "./LloaModelField";


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
        <LloaModelField
          value={modelId}
          onChange={(v) => setValue("modelId", v, { shouldValidate: true })}
          errorMessage={errors.modelId?.message}
        />
      </FieldGroup>
    </form>
  );
}
