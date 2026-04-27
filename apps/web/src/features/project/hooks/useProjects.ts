import { useQuery } from "@tanstack/react-query";
import { projectsApi } from "../api/project.api";
import { projectKeys } from "../constants/queryKeys";
import { mapProject } from "../api/project.mapper";

export const useProjects = () =>
  useQuery({
    queryKey: projectKeys.lists(),
    queryFn: async () => {
      const data = await projectsApi.getProjects();
      return data.map(mapProject);
    },
  });

export const useProjectDetail = (id: string) =>
  useQuery({
    queryKey: projectKeys.detail(id),
    queryFn: async () => {
      const data = await projectsApi.getProjectById(id);
      return mapProject(data);
    },
    enabled: !!id,
  });
