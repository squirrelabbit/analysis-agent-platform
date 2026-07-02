import type { Project } from "./model";
import type { ProjectResponse } from "./dto";
import type { ProjectFormValues } from "../schemas/project.schema";

export const mapProject = (dto: ProjectResponse): Project => ({
  id: dto.project_id,
  name: dto.name,
  description: dto.description,
  datasetCount: dto.dataset_count ?? dto.dataset_version_count,
  promptCount: dto.prompt_count,
  chatCount: dto.analysis_thread_count,
  createdAt: dto.created_at
})

// 상세 응답 → 수정 폼 값. 폼은 문자열로 다루므로 숫자(연도)를 문자열로 변환한다.
// 옛 모델(before/after_days)로 저장된 프로젝트는 target/role이 없으므로 기본값으로 채운다.
export const projectToFormValues = (dto: ProjectResponse): ProjectFormValues => {
  const festival = dto.metadata?.festival;
  return {
    name: dto.name,
    description: dto.description,
    festivalName: festival?.name ?? "",
    periods: (festival?.periods ?? []).map((p) => ({
      year: String(p.year),
      role: p.role === "base" ? "base" : "compare",
      target_start: p.target_start ?? "",
      target_end: p.target_end ?? "",
      festival_start: p.festival_start ?? "",
      festival_end: p.festival_end ?? "",
    })),
  };
};
