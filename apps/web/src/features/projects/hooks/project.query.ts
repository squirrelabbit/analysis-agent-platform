import { useQuery } from "@tanstack/react-query";
import { projectKeys } from "../api/project.keys";
import { projectsApi } from "../api/project.api";
import { mapProject } from "../models/mapper";

export const useProjects = () =>
  useQuery({
    queryKey: projectKeys.lists(),
    queryFn: projectsApi.getProjects,
    select: (data) => data.map(mapProject),
  });

export const useProjectDetail = (projectId: string) =>
  useQuery({
    queryKey: projectKeys.detail(projectId),
    queryFn: () => projectsApi.getProjectById(projectId),
    enabled: !!projectId,
    select: mapProject,
  });

// 수정 폼 프리필용 — metadata(축제)까지 포함한 원본 응답을 그대로 쓴다.
// useProjectDetail과 같은 캐시 키를 공유하되 select만 다르다.
export const useProjectRaw = (projectId: string, enabled = true) =>
  useQuery({
    queryKey: projectKeys.detail(projectId),
    queryFn: () => projectsApi.getProjectById(projectId),
    enabled: enabled && !!projectId,
  });
