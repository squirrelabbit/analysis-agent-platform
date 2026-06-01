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
        // 따옴표 캡처 회피: 따옴표 안/밖을 분리해 매칭.
        // (기존 /filename="?(.+)"?/ 는 .+ greedy로 끝 " 까지 캡처해 파일명 끝에
        //  "가 들어가고 OS가 _로 sanitize → clause_label.csv_ 되는 버그.)
        const match = disposition.match(/filename="([^"]+)"|filename=([^;]+)/);
        if (match) filename = (match[1] ?? match[2] ?? filename).trim();
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
