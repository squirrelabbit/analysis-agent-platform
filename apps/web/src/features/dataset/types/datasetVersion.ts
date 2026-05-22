export type Stage = 'clean' | 'docGenuineness' | 'clauseLabel'

export interface CleanSummary {
  inputRowCount: number;
  outputRowCount: number;
  keptCount: number;
  droppedCount: number;
  textColumns: string[];
  textJoiner: string;
  preprocessOptions: {
    removeEnglish: false;
    removeMonosyllables: false;
    removeNumbers: false;
    removeSpecial: false;
  };
  sourceInputCharCount: number;
  cleanedInputCharCount: number;
  cleanReducedCharCount: number;
  cleanRegexRuleHits: {
    htmlArtifact: number;
    mediaPlaceholder: number;
    urlCleanup: number;
  };
}

export interface DocGenuinenessSummary {
  inputArtifactRef: string;
  inputRowCount: number;
  model: string;
  parseFailures: number;
  processedRowCount: number;
  promptVersion: string;
  tierCounts: {
    genuineReview: number;
    mixed: number;
    nonReview: number;
  };
  totalCompletionTokens: number;
  totalPromptTokens: number;
}

export interface ClauseLabelSummary {
  clauseCount: number;
  includeGenuineness: string[];
  inputArtifactRef: string;
  inputRowCount: number;
  model: string;
  parseFailures: number;
  processedDocCount: number;
  promptVersion: string
  sentimentCounts: {
    negative: number;
    neutral: number;
    positive: number;
    mixed: number;
  };
  skippedByFilter: number;
  skippedEmpty: number;
  totalCompletionTokens: number;
  totalPromptTokens: number;
}

export interface BuildStageResult<T = unknown> {
  status: string
  completedAt?: string
  summary?: T
}

export interface DatasetVersion {
  id: string,
  createdAt: string;
  isActive: boolean,
  rowCount: number;
  columnCount: number;
  columns: string[],
  byteSize: number;
  cleanStatus: string,
  docGenuinenessStatus: string,
  clauseLabelStatus: string,
  originalFilename: string
}

export interface DatasetVersionDetail {
  id: string,
  createdAt: string;
  isActive: boolean,
  rowCount: number;
  columnCount: number;
  columns: string[],
  byteSize: number;
  clean: BuildStageResult;
  docGenuineness: BuildStageResult;
  clauseLabel: BuildStageResult;
}