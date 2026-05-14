import { useMutation, useQueryClient } from "@tanstack/react-query"
import { projectsApi } from "../api/project.api";
import { projectKeys } from "../api/project.keys";

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

export const useDeleteProjectMutation = () => {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: (projectId: string) => projectsApi.deleteProject(projectId),

    onSuccess: (_, projectId) => {
      queryClient.removeQueries({
        queryKey: projectKeys.detail(projectId)
      })
    }
  })
}
