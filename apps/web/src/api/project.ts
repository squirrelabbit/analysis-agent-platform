import { apiClient } from "./client";
import type {
  CreateProjectPayload,
  ProjectResponse,
  ProjectListResponse,
} from "@/types/dto/project.dto";
import type { AxiosResponse } from "axios";

export const projectsApi = {
  getAll: () =>
    apiClient
      .get<ProjectListResponse>("/projects")
      .then((r: AxiosResponse<ProjectListResponse>) => r.data.items),

  getById: (id: string) =>
    apiClient.get<ProjectResponse>(`/projects/${id}`).then((r) => r.data),

  create: (payload: CreateProjectPayload) =>
    apiClient.post<ProjectResponse>("/projects", payload).then((r) => r.data),
};
