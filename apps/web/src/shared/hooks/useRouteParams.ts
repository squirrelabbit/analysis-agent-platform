import { useRequiredParams } from "./useRequiredParams";

export function useProjectParams() {
  return useRequiredParams<{
    projectId: string;
  }>(["projectId"]);
}

export function useDatasetParams() {
  return useRequiredParams<{
    projectId: string;
    datasetId: string;
  }>(["projectId", "datasetId"]);
}

export function useVersionParams() {
  return useRequiredParams<{
    projectId: string;
    datasetId: string;
    versionId: string;
  }>(["projectId", "datasetId", "versionId"]);
}
