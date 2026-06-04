import type { Pagination } from "@/shared/models/common";
import type { BuildJobType } from "@/shared/types/common";


export interface ProgressType {
  percent: number;
  processedRows: number;
  totalRows: number;
  message: string;
  updatedAt: string;
}

export interface BuildBase<TType extends BuildJobType, TSummary> {
  buildType: TType;
  status: string;
  jobId: string;
  startedAt: string;
  completedAt: string;
  durationSeconds: number;
  errorMessage: string;
  progress?: ProgressType;
  summary?: TSummary;
}

export interface PaginatedSummary<T> {
  items: T[];
  pagination: Pagination;
  applied: { promptVersion: string };
}

export interface CleanSummary {
  cleanReducedCharCount: number;
  cleanedInputCharCount: number;
  droppedCount: number;
  inputRowCount: number;
  keptCount: number;
  outputRowCount: number;
  sourceInputCharCount: number;
  textColumn: string;
  textColumns: string[];
}

export interface GenuinenessItem {
  docId: string;
  genuineness: string;
  reason: string;
  source: string;
  cleanedText: string;
}

export interface GenuinenessSummary  {
  genuineness: {
    genuineReview: number;
    nonReview: number;
    uncertain: number;
    mixed: number;
  };
  total: number;
}

export interface ClauseItem {
  aspect: string;
  clause: string;
  clauseId: string;
  docId: string;
  sentiment: string;
  source: string;
}

export interface ClauseSummary {
  // aspect key(snake_case, taxonomy 기반) → 건수. key 집합은 taxonomy config에
  // 따라 달라지므로 고정 필드가 아닌 동적 맵으로 둔다.
  aspect: Record<string, number>;
  sentiment: {
    positive: number;
    negative: number;
    neutral: number;
  };
  total: number;
}

export type CleanBuild = BuildBase<"clean", CleanSummary>;
export type GenuinenessBuild = BuildBase<"doc_genuineness", GenuinenessSummary> & PaginatedSummary<GenuinenessItem>;
export type ClauseBuild = BuildBase<"clause_label", ClauseSummary> & PaginatedSummary<ClauseItem>;

export type Build = CleanBuild | GenuinenessBuild | ClauseBuild;


export interface VersionBuild<T> {
  status: string,
  completedAt?: string,
  summary?: T
}

export type CleanVersionBuild = VersionBuild<CleanSummary>;
export type GenuinenessVersionBuild = VersionBuild<GenuinenessSummary>;
export type ClauseVersionBuild = VersionBuild<ClauseSummary>;
