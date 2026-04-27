import {
  Field,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import {
  datasetSchema,
  type DatasetFormValues,
} from "../../schcema/dataset.schcema";

export default function CreateDatasetForm({
  formId,
  onSubmit,
  onSuccess,
}: {
  formId: string;
  onSubmit: (data: DatasetFormValues) => Promise<void>;
  onSuccess: () => void;
}) {
  const {
    register,
    handleSubmit,
    setValue,
    watch,
    formState: { errors },
  } = useForm<DatasetFormValues>({
    resolver: zodResolver(datasetSchema),
    defaultValues: {
      dataType: "unstructured",
    },
  });

  const dataType = watch("dataType");

  const handleFormSubmit = async (data: DatasetFormValues) => {
    await onSubmit(data);
    onSuccess();
  };

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-6 py-5">
        <Field>
          <FieldLabel className="text-xs">
            데이터셋 이름 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input {...register("name")} placeholder="예) sns 데이터" />
          {errors.name && (
            <p className="text-xs text-red-500">{errors.name.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            설명 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input
            {...register("description")}
            placeholder="데이터셋에 대한 간단한 설명"
          />
          {errors.description && (
            <p className="text-xs text-red-500">{errors.description.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            타입 <span className="text-red-500">*</span>
          </FieldLabel>

          <RadioGroup
            value={dataType}
            onValueChange={(v) =>
              setValue("dataType", v as "structured" | "unstructured")
            }
          >
            <FieldLabel>
              <Field orientation="horizontal">
                <RadioGroupItem value="structured" />
                <FieldTitle>정형</FieldTitle>
              </Field>
            </FieldLabel>

            <FieldLabel>
              <Field orientation="horizontal">
                <RadioGroupItem value="unstructured" />
                <FieldTitle>비정형</FieldTitle>
              </Field>
            </FieldLabel>
          </RadioGroup>

          {errors.dataType && (
            <p className="text-xs text-red-500">{errors.dataType.message}</p>
          )}
        </Field>
      </FieldGroup>
    </form>
  );
}
