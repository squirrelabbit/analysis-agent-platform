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
        // 한글 등은 RFC 5987 filename*(UTF-8 percent-encoded)를 우선 읽어 decode한다.
        // (헤더는 latin-1로 해석돼 raw UTF-8 filename=은 깨지므로 filename*가 정답.)
        const star = disposition.match(/filename\*=UTF-8''([^;]+)/i);
        if (star) {
          try {
            filename = decodeURIComponent(star[1].trim());
          } catch {
            filename = star[1].trim();
          }
        } else {
          // fallback: 따옴표 안/밖을 분리해 매칭(.+ greedy로 끝 " 캡처 버그 회피).
          const match = disposition.match(/filename="([^"]+)"|filename=([^;]+)/);
          if (match) filename = (match[1] ?? match[2] ?? filename).trim();
        }
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
