export const datasetKeys = {
  all: ['datasets'] as const,
  lists: () => [...datasetKeys.all, 'list'] as const,
  detail: (projectId: string, datasetId: string) => [...datasetKeys.all, 'detail', projectId, datasetId] as const
}
