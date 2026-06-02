import type { FormProps } from "@/shared/models/common";
import { BuildGenuinenessSchema, type BuildGenuinenessFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { FieldGroup } from "@/components/ui/field";
import PromptVersionField from "@/features/prompts/components/PromptVersionField";


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

  async function handleFormSubmit(data: BuildGenuinenessFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <PromptVersionField
          task="doc_genuineness"
          value={promptVersion}
          onChange={(v) =>
            setValue("promptVersion", v, { shouldValidate: true })
          }
          errorMessage={errors.promptVersion?.message}
        />
      </FieldGroup>
    </form>
  );
}
