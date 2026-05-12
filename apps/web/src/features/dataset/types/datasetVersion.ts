export type VersionRouteParams = {
  projectId: string
  datasetId: string
  versionId: string
}

export interface SourceSummary {
  available: boolean;
  status: string; // "ready";
  format: string;
  rowCount: number;
  columnCount: number;
  columns: Record<string, any>[];
  sampleLimit: number;
  sampleRows: Record<string, any>[];
  errorMessage?: string;
}


export interface CleanSummary {
  inputRowCount: number;
  outputRowCount: number;
  keptCount: number;
  droppedCount: number;
  textColumns: string[];
  textJoiner: string;
  sourceInputCharCount: number;
  cleanedInputCharCount: number;
  cleanReducedCharCount: number;
}

export interface PrerpareSummary {
  inputRowCount: number;
  outputRowCount: number;
  keptCount: number;
  reviewCount: number;
  droppedCount: number;
  textColumn: string;
  textColumns: string[];
  textJoiner: string;
}

export interface Artifact {
  artifactId: string,
  projectId: string,
  datasetId: string,
  datasetVersionId: string,
  artifactType: string,
  stage: string,
  status: string,
  uri: string,
  format: string,
  model?: string,
  promptVersion?: string
  metadata: any
  createdAt: string
  updatedAt: string
}

export interface ProgressType {
  percent: number;
  processedRows: number;
  totalRows: number;
  elapsedSeconds: number;
  message: string;
  updatedAt: string
}

export interface Diagnostics {
  retryCount: number,
  workflowId: string,
  workflowRunId: string,
  resumedExecutionCount: number,
  progress?: ProgressType
}

export interface BuildStage {
  stage: string;
  status: string;
  applicable: boolean;
  required: boolean;
  ready: boolean;
  dependsOn: string[]
  canRun: boolean;
  runGroup: string;
  autoRuEligible: boolean;
  blockedReason?:string;
  latestJob?: Record<string, any>,
  primaryArtifact?: Record<string, any>,
  artifacts?: Artifact[];
  summary?: Record<string, any>
  model?: string,
  promptVersion?:string,
  diagnostics?: Diagnostics
}


export interface DatasetVersion {
  id: string,
  datasetId: string;
  projectId: string,
  metadata: any,
  storageUri: string,
  dataType: string,
  recordCount: number,
  sourceSummary: SourceSummary
  buildStages: BuildStage[],
  isActive: boolean,
  cleanStatus: string,
  cleanSummary?: CleanSummary,
  prepareStatus: string,
  prepareSummary?: PrerpareSummary,
  sentimentStatus: string,
  embeddingStatus: string,
}