
export interface Dataset {
  id: string,
  projectId: string,
  name: string,
  description: string,
  dataType: 'structured' | 'unstructured',
  activeDatasetVersionId: string,
  activeVersionUpdatedAt: string,
  createdAt: string
}
