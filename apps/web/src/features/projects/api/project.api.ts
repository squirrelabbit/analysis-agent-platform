import { apiClient } from "@/api/client";
import type {
  CreateProjectRequest,
  ProjectListResponse,
  ProjectResponse,
} from "../models/dto";

export const projectsApi = {
  getProjects: () =>
    apiClient.get<ProjectListResponse>(`/projects`).then((r) => r.data.items),

  getProjectById: (id: string) =>
    apiClient.get<ProjectResponse>(`/projects/${id}`).then((r) => r.data),

  createProject: (req: CreateProjectRequest) =>
    apiClient.post<ProjectResponse>(`/projects`, req).then((r) => r.data),
  
  deleteProject: (id: string) =>
    apiClient.delete<void>(`/projects/${id}`).then((r) => r.data),
};
