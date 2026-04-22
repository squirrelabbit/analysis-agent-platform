import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { projectsApi } from "../api/project.api";
import { projectKeys } from "../constants/queryKeys";
import { mapProject } from "../api/project.mapper";

export const useProjects = () => {
  const queryClient = useQueryClient();

  const query = useQuery({
    queryKey: projectKeys.lists(),
    queryFn: async () => {
      const data = await projectsApi.getProjects()
      return data.map(mapProject)
    },
  });

  const create = useMutation({
    mutationFn: projectsApi.createProject,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['projects'] })
    },
  });

  const remove = useMutation({
    mutationFn: projectsApi.deleteProject,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["projects"] });
    },
  });

  return {
    ...query,
    create,
    remove,
  };
};
