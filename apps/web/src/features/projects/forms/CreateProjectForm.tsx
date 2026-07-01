import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { useFieldArray, useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import {
  projectSchema,
  type ProjectFormValues,
} from "../schemas/project.schema";
import type {
  CreateProjectRequest,
  FestivalPeriodInput,
} from "../models/dto";

// 폼 값(문자열) → CreateProjectRequest. 축제명이 있을 때만 festival을 실어 보낸다.
// before/after_days는 >0일 때만 포함(비우거나 0이면 개방형).
function buildCreateRequest(data: ProjectFormValues): CreateProjectRequest {
  const req: CreateProjectRequest = {
    name: data.name.trim(),
    description: data.description.trim(),
  };
  const festivalName = data.festivalName?.trim();
  if (festivalName) {
    const periods = (data.periods ?? [])
      .filter((p) => p.year || p.festival_start || p.festival_end)
      .map((p) => {
        const period: FestivalPeriodInput = {
          year: Number(p.year),
          festival_start: p.festival_start,
          festival_end: p.festival_end,
        };
        const before = Number(p.before_days);
        const after = Number(p.after_days);
        if (before > 0) period.before_days = before;
        if (after > 0) period.after_days = after;
        return period;
      });
    req.metadata = { festival: { name: festivalName, periods } };
  }
  return req;
}

export default function CreateProjectForm({
  formId,
  onSubmit,
  onSuccess,
  defaultValues,
}: {
  formId: string;
  onSubmit: (data: CreateProjectRequest) => Promise<void>;
  onSuccess: () => void;
  // 수정 진입점에서 기존 프로젝트 값을 프리필할 때 넘긴다. 미지정 시 빈 폼(생성).
  defaultValues?: ProjectFormValues;
}) {
  const {
    register,
    control,
    handleSubmit,
    formState: { errors },
  } = useForm<ProjectFormValues>({
    resolver: zodResolver(projectSchema),
    defaultValues: defaultValues ?? {
      name: "",
      description: "",
      festivalName: "",
      periods: [],
    },
  });

  const { fields, append, remove } = useFieldArray({ control, name: "periods" });

  const handleFormSubmit = async (data: ProjectFormValues) => {
    await onSubmit(buildCreateRequest(data));
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

        {/* 축제 정보(#31) — 선택. 축제명을 넣으면 분석 개요의 분석 대상/기간이 된다. */}
        <div className="mt-1 rounded-md border border-[#e6e8f0] p-3">
          <p className="mb-2 text-xs font-semibold text-[#16192b]">
            축제 정보 <span className="font-normal text-[#9399b0]">(선택)</span>
          </p>
          <Field>
            <FieldLabel className="text-xs">축제명</FieldLabel>
            <Input {...register("festivalName")} placeholder="예) 강릉야행문화축제" />
            {errors.festivalName && (
              <p className="text-xs text-red-500">{errors.festivalName.message}</p>
            )}
          </Field>

          <div className="mt-3 flex flex-col gap-3">
            {fields.map((field, idx) => (
              <div
                key={field.id}
                className="rounded-md bg-[#f7f8fc] p-3"
              >
                <div className="mb-2 flex items-center justify-between">
                  <span className="text-xs font-medium text-[#5b6178]">
                    연도별 축제 기간 #{idx + 1}
                  </span>
                  <Button
                    type="button"
                    variant="ghost"
                    size="sm"
                    className="h-6 px-2 text-xs text-red-500"
                    onClick={() => remove(idx)}
                  >
                    삭제
                  </Button>
                </div>
                <div className="grid grid-cols-3 gap-2">
                  <Field>
                    <FieldLabel className="text-[11px]">연도</FieldLabel>
                    <Input
                      type="number"
                      {...register(`periods.${idx}.year` as const)}
                      placeholder="2025"
                    />
                    {errors.periods?.[idx]?.year && (
                      <p className="text-[11px] text-red-500">
                        {errors.periods[idx]?.year?.message}
                      </p>
                    )}
                  </Field>
                  <Field>
                    <FieldLabel className="text-[11px]">축제 시작일</FieldLabel>
                    <Input
                      type="date"
                      {...register(`periods.${idx}.festival_start` as const)}
                    />
                    {errors.periods?.[idx]?.festival_start && (
                      <p className="text-[11px] text-red-500">
                        {errors.periods[idx]?.festival_start?.message}
                      </p>
                    )}
                  </Field>
                  <Field>
                    <FieldLabel className="text-[11px]">축제 종료일</FieldLabel>
                    <Input
                      type="date"
                      {...register(`periods.${idx}.festival_end` as const)}
                    />
                    {errors.periods?.[idx]?.festival_end && (
                      <p className="text-[11px] text-red-500">
                        {errors.periods[idx]?.festival_end?.message}
                      </p>
                    )}
                  </Field>
                </div>
                <div className="mt-2 grid grid-cols-2 gap-2">
                  <Field>
                    <FieldLabel className="text-[11px]">
                      축제 전 N일 <span className="text-[#9399b0]">(비우면 전체)</span>
                    </FieldLabel>
                    <Input
                      type="number"
                      min={0}
                      {...register(`periods.${idx}.before_days` as const)}
                      placeholder="예) 3"
                    />
                  </Field>
                  <Field>
                    <FieldLabel className="text-[11px]">
                      축제 후 N일 <span className="text-[#9399b0]">(비우면 전체)</span>
                    </FieldLabel>
                    <Input
                      type="number"
                      min={0}
                      {...register(`periods.${idx}.after_days` as const)}
                      placeholder="예) 3"
                    />
                  </Field>
                </div>
              </div>
            ))}
          </div>

          <Button
            type="button"
            variant="outline"
            size="sm"
            className="mt-3 w-full text-xs"
            onClick={() =>
              append({
                year: "",
                festival_start: "",
                festival_end: "",
                before_days: "",
                after_days: "",
              })
            }
          >
            + 연도 추가
          </Button>
        </div>
      </FieldGroup>
    </form>
  );
}
