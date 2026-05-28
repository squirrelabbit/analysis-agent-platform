export const datasetKeys = {
  all: ['datasets'] as const,
  lists: () => [...datasetKeys.all, 'list'] as const,
  list: (projectId: string) => [...datasetKeys.lists(), projectId] as const,
  details: () => [...datasetKeys.all, 'detail'] as const,
  detail: (projectId: string, datasetId: string) =>
    [...datasetKeys.details(), projectId, datasetId] as const,
}
