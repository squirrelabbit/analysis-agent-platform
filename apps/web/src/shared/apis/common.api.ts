import { apiClient } from "@/api/client";
import type { BuildJobType } from "../types/common";

export const downloadApi = {
  downloadFile: (projectId: string, datasetId: string, versionId: string, type: BuildJobType)=>
    apiClient.get(`/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}_download`, { responseType: "blob" })
}