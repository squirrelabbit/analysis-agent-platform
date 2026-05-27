import type { FormProps } from "@/shared/model/common";
import { BuildGenuinenessSchema, type BuildGenuinenessFormValues } from "../../schemas/build.schema";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";



export default function BuildGenuinenessForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildGenuinenessFormValues>) {
  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<BuildGenuinenessFormValues>({
    resolver: zodResolver(BuildGenuinenessSchema),
  });

  async function handleFormSubmit(data: BuildGenuinenessFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <Field>
          <FieldLabel className="text-xs">
            프롬프트 버전<p className="text-xs text-zinc-300">(선택)</p>
          </FieldLabel>
          <Input {...register("promptVersion")} />
          {errors.promptVersion && (
            <p className="text-xs text-red-500">
              {errors.promptVersion.message}
            </p>
          )}
        </Field>
      </FieldGroup>
    </form>
  );
}
