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
