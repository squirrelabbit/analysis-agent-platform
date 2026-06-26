export const reportKeys = {
  all: ["reports"] as const,

  // 보고서 문서(Report) 캐시.
  documents: () => [...reportKeys.all, "documents"] as const,
  documentList: (projectId: string) =>
    [...reportKeys.documents(), "list", projectId] as const,
  document: (projectId: string, reportId: string) =>
    [...reportKeys.documents(), projectId, reportId] as const,
};
