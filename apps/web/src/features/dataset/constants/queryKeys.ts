export const datasetKeys = {
  all: ['datasets'] as const,
  lists: () => [...datasetKeys.all, 'list'] as const,
  detail: (id: string) => [...datasetKeys.all, 'detail', id] as const
}