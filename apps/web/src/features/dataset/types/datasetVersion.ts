
export type LLMMode = 'default' | 'enabled' | 'disabled'

export interface DatasetVersion {
  id: string,
  datasetId: string,
  projectId: string,
  storageUri: string,
  dataType: string,
  recordCount: number,
  metadata: Record<string, any>,
  profile: Record<string, any>,
  prepareStatus: string,
  prepareLLMMode: string, // default
  prepareModel: string,
  preparePromptVersion: string,
  prepareUri: string,
  preparedAt: string,
  prepareSummary: Record<string, any>,
  sentimentStatus: string,
  sentimentLLMMode: string, // default
  sentimentModel: string,
  sentimentUri: string,
  sentimentLabeledAt: string,
  sentimentPromptVersion: string,
  embeddingStatus: string,
  embeddingModel: string,
  embeddingUri: string,
  isActive: boolean,
  createdAt: string,
  readyAt: string
}