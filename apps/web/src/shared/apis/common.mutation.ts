import { useMutation } from "@tanstack/react-query";
import { useDatasetParams } from "../hooks/useRouteParams";
import { downloadApi } from "./common.api";
import type { BuildJobType } from "../types/common";

export const useDownloadFile = () => {
  const { projectId, datasetId } = useDatasetParams();
  return useMutation({
    mutationFn: ({ versionId, type }: { versionId: string; type: BuildJobType }) =>
      downloadApi.downloadFile(projectId, datasetId, versionId, type),
    onSuccess: ({ headers, data }) => {
      let filename = "download";
      // 파일명 추출
      const disposition = headers["content-disposition"];

      if (disposition) {
        const match = disposition.match(/filename="?(.+)"?/);
        if (match) filename = match[1];
      }

      const url = window.URL.createObjectURL(data);

      const a = document.createElement("a");
      a.href = url;
      a.download = filename;

      document.body.appendChild(a);
      a.click();
      a.remove();

      window.URL.revokeObjectURL(url);
    },
  });
};
