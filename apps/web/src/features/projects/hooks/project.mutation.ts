import { useMutation, useQueryClient } from "@tanstack/react-query"
import { projectsApi } from "../api/project.api";
import { projectKeys } from "../api/project.keys";
import type { UpdateProjectRequest } from "../models/dto";

export const useCreateProjectMutation = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: projectsApi.createProject,

    onSuccess: () => {
      queryClient.invalidateQueries({
        queryKey: projectKeys.lists(),
      });
    },
  })
}

export const useUpdateProjectMutation = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ id, req }: { id: string; req: UpdateProjectRequest }) =>
      projectsApi.updateProject(id, req),

    onSuccess: (_, { id }) => {
      queryClient.invalidateQueries({ queryKey: projectKeys.detail(id) });
      queryClient.invalidateQueries({ queryKey: projectKeys.lists() });
    },
  });
};

export const useDeleteProjectMutation = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (projectId: string) => projectsApi.deleteProject(projectId),

    onSuccess: (_, projectId) => {
      queryClient.removeQueries({
        queryKey: projectKeys.detail(projectId)
      })
      queryClient.invalidateQueries({
        queryKey: projectKeys.lists(),
      })
    }
  })
}
