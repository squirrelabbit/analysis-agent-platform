import { useMutation, useQueryClient } from "@tanstack/react-query";
import { datasetVersionsApi } from "../api/datasetVersion.api";
import type { UploadDatasetVersionRequest } from "../types/datasetVersion.dto";

export const useActiveVersion = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      versionId,
    }: {
      projectId: string;
      datasetId: string;
      versionId: string;
    }) =>
      datasetVersionsApi.activeDatasetVersion(projectId, datasetId, versionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};

export const useUploadVersionMutation = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      req,
    }: {
      projectId: string;
      datasetId: string;
      req: UploadDatasetVersionRequest;
    }) => datasetVersionsApi.uploadDatasetVersion(projectId, datasetId, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};

export const useDownloadVersion = () => {
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      versionId,
      type,
    }: {
      projectId: string;
      datasetId: string;
      versionId: string;
      type: "source" | "clean" | "prepare" | "sentiment";
    }) =>
      datasetVersionsApi.downloadVersionFile(
        projectId,
        datasetId,
        versionId,
        type,
      ),
    onSuccess: (res) => {
      const blob = res.data;

      // 파일명 추출
      const disposition = res.headers["content-disposition"];

      let fileName = "download";

      if (disposition) {
        const match = disposition.match(/filename="?(.+)"?/);
        if (match) fileName = match[1];
      }

      const url = window.URL.createObjectURL(blob);

      const a = document.createElement("a");
      a.href = url;
      a.download = fileName;

      document.body.appendChild(a);
      a.click();
      a.remove();

      window.URL.revokeObjectURL(url);
    },
  });
};

export const useRemoveVersion = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      versionId,
    }: {
      projectId: string;
      datasetId: string;
      versionId: string;
    }) =>
      datasetVersionsApi.deleteDatasetVersion(projectId, datasetId, versionId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};

export const useRunBuildJob = () => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      projectId,
      datasetId,
      versionId,
      type,
    }: {
      projectId: string;
      datasetId: string;
      versionId: string;
      type: "segment" | "clause_label" | "embedding_cluster" | "keyword_index";
    }) => datasetVersionsApi.runBuildJob(projectId, datasetId, versionId, type),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["versions"] });
    },
  });
};
