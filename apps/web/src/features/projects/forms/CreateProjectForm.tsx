import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import {
  projectSchema,
  type ProjectFormValues,
} from "../schemas/project.schema";

export default function CreateProjectForm({
  formId,
  onSubmit,
  onSuccess,
}: {
  formId: string;
  onSubmit: (data: ProjectFormValues) => Promise<void>;
  onSuccess: () => void;
}) {
  const {
    register,
    handleSubmit,
    formState: { errors },
  } = useForm<ProjectFormValues>({
    resolver: zodResolver(projectSchema),
  });

  const handleFormSubmit = async (data: ProjectFormValues) => {
    await onSubmit(data);
    onSuccess(); // 성공하면 닫기
  };
  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-6 py-5">
        <Field>
          <FieldLabel className="text-xs">
            프로젝트 이름 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input {...register("name")} placeholder="festival" />
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
            placeholder="프로젝트에 대한 간단한 설명"
          />
          {errors.description && (
            <p className="text-xs text-red-500">{errors.description.message}</p>
          )}
        </Field>
      </FieldGroup>
    </form>
  );
}
