export const datasetKeys = {
  all: ['datasets'] as const,
  lists: () => [...datasetKeys.all, 'list'] as const,
  detail: (id: string) => [...datasetKeys.all, 'detail', id] as const
}

export const datasetVersionKeys = {
  all: ['versions'] as const,
  lists: () => [...datasetVersionKeys.all, 'list'] as const,
  detail: (projectId: string, datasetId: string, versionId: string) => [...datasetVersionKeys.all, 'detail', projectId, datasetId, versionId] as const,
}