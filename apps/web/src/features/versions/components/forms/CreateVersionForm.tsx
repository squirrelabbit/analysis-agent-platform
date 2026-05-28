import type { DataType } from "@/shared/types/common";
import {
  versionSchema,
  type VersionFormValues,
} from "../../schemas/version.schema";
import { zodResolver } from "@hookform/resolvers/zod";
import { Controller, useForm } from "react-hook-form";
import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
} from "@/components/ui/field";
import FileUploader from "@/components/common/files/FileUploader";
import type { FormProps } from "@/shared/models/common";
import { Checkbox } from "@/components/ui/checkbox";


export default function CreateVersionForm({
  formId,
  type,
  onSubmit,
  onSuccess,
}: FormProps<VersionFormValues> & { type: DataType }) {
  const {
    handleSubmit,
    control,
    formState: { errors },
  } = useForm<VersionFormValues>({
    resolver: zodResolver(versionSchema),
    defaultValues: {
      dataType: type,
      activateOnCreate: true,
    },
  });

  async function handleFormSubmit(data: VersionFormValues) {
    console.log("submit", data);
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
        <Controller
          control={control}
          name="activateOnCreate"
          render={({ field }) => (
            <Field orientation="horizontal">
              <Checkbox
                id="activate-checkbox"
                checked={field.value}
                onCheckedChange={field.onChange}
              />

              <FieldContent>
                <FieldLabel htmlFor="activate-checkbox">활성화</FieldLabel>

                <FieldDescription className="text-xs">
                  업로드한 데이터를 현재 활성 데이터셋으로 즉시 적용합니다.
                </FieldDescription>
              </FieldContent>
            </Field>
          )}
        />
      </FieldGroup>
    </form>
  );
}
