import { useQuery } from "@tanstack/react-query";
import { reportApi } from "../api/report.api";
import { reportKeys } from "../api/report.key";
import { mapReportSavedResultList } from "../models";

// 보고서 보관함 목록. datasetId 지정 시 해당 dataset만, 미지정 시 project 전체.
export const useSavedResults = (projectId: string, datasetId?: string) =>
  useQuery({
    queryKey: reportKeys.savedResultList(projectId, datasetId),
    queryFn: () => reportApi.listSavedResults(projectId, datasetId),
    enabled: !!projectId,
    select: mapReportSavedResultList,
  });
