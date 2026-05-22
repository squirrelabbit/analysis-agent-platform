import { compactObject } from "@/utils/clean";
import type { UploadVersionFormValues } from "../schcema/dataset.schcema";
import type { CleanJobFormValues } from "../schcema/version.schema";
import type {
  ClauseLabelSummary,
  CleanSummary,
  DatasetVersion,
  DatasetVersionDetail,
  DocGenuinenessSummary,
} from "../types/datasetVersion";
import type {
  ClauseLabelSummaryDto,
  CleanJobPayload,
  CleanSummaryDto,
  DatasetVersionDetailResponse,
  DatasetVersionResponse,
  DocGenuinenessSummaryDto,
  UploadDatasetVersionRequest,
} from "../types/datasetVersion.dto";

export const mapDatasetVersion = (
  dto: DatasetVersionResponse,
): DatasetVersion => ({
  id: dto.dataset_version_id,
  createdAt: dto.created_at,
  isActive: dto.is_active,
  rowCount: dto.row_count,
  columnCount: dto.column_count,
  columns: dto.columns,
  byteSize: dto.byte_size,
  cleanStatus: dto.clean_status,
  docGenuinenessStatus: dto.doc_genuineness_status,
  clauseLabelStatus: dto.clause_label_status,
  originalFilename: dto.original_filename
});

export const mapDatasetVersionDetail = (
  dto: DatasetVersionDetailResponse,
): DatasetVersionDetail => ({
  id: dto.dataset_version_id,
  createdAt: dto.created_at,
  isActive: dto.is_active,
  rowCount: dto.row_count,
  columnCount: dto.column_count,
  columns: dto.columns,
  byteSize: dto.byte_size,
  clean: {
    status: dto.clean.status,
    completedAt: dto.clean.completed_at,
    summary: dto.clean.summary ? mapCleanSummary(dto.clean.summary as CleanSummaryDto) : undefined
  },
  docGenuineness: {
    status: dto.doc_genuineness.status,
    completedAt: dto.doc_genuineness.completed_at,
    summary: dto.doc_genuineness.summary ? mapDocGenuinenessSummary(dto.doc_genuineness.summary as DocGenuinenessSummaryDto) : undefined
  },
  clauseLabel: {
    status: dto.clause_label.status,
    completedAt: dto.clause_label.completed_at,
    summary: dto.clause_label.summary ? mapClauseLabelSummary(dto.clause_label.summary as ClauseLabelSummaryDto) : undefined
  },
});

export const mapUploadFormToRequest = (
  form: UploadVersionFormValues,
): UploadDatasetVersionRequest => ({
  file: form.file,
  data_type: form.dataType,
  activate_on_create: form.activateOnCreate,
});

export const mapCleanSummary = (dto: CleanSummaryDto): CleanSummary => ({
  inputRowCount: dto.input_row_count ?? 0,
  outputRowCount: dto.output_row_count ?? 0,
  keptCount: dto.kept_count ?? 0,
  droppedCount: dto.dropped_count ?? 0,
  textColumns: dto.text_columns,
  textJoiner: dto.text_joiner,
  preprocessOptions:  {
    removeEnglish: dto.preprocess_options?.remove_english,
    removeMonosyllables: dto.preprocess_options?.remove_monosyllables,
    removeNumbers: dto.preprocess_options?.remove_numbers,
    removeSpecial: dto.preprocess_options?.remove_special,
  },
  sourceInputCharCount: dto.source_input_char_count ?? 0,
  cleanedInputCharCount: dto.cleaned_input_char_count ?? 0,
  cleanReducedCharCount: dto.clean_reduced_char_count ?? 0,
  cleanRegexRuleHits: {
    htmlArtifact: dto.clean_regex_rule_hits?.html_artifact ?? 0,
    mediaPlaceholder: dto.clean_regex_rule_hits?.media_placeholder ?? 0,
    urlCleanup: dto.clean_regex_rule_hits?.url_cleanup ?? 0,
  }
});

export const mapDocGenuinenessSummary  = (dto: DocGenuinenessSummaryDto): DocGenuinenessSummary => ({
  inputArtifactRef: dto.input_artifact_ref,
  inputRowCount: dto.input_row_count ?? 0,
  model: dto.model,
  parseFailures: dto.parse_failures ?? 0,
  processedRowCount: dto.processed_row_count ?? 0,
  promptVersion: dto.prompt_version,
  tierCounts: {
    genuineReview: dto.tier_counts?.genuine_review ?? 0,
    mixed: dto.tier_counts?.mixed ?? 0,
    nonReview: dto.tier_counts?.non_review ?? 0
  },
  totalCompletionTokens: dto.total_completion_tokens ?? 0,
  totalPromptTokens: dto.total_prompt_tokens ?? 0,
});

export const mapClauseLabelSummary  = (dto: ClauseLabelSummaryDto): ClauseLabelSummary => ({
  clauseCount: dto.clause_count ?? 0,
  includeGenuineness: dto.include_genuineness,
  inputArtifactRef: dto.input_artifact_ref,
  inputRowCount: dto.input_row_count,
  model: dto.model,
  parseFailures: dto.parse_failures ?? 0,
  processedDocCount: dto.processed_doc_count ?? 0,
  promptVersion: dto.prompt_version,
  sentimentCounts: {
    negative: dto.sentiment_counts.negative ?? 0,
    neutral: dto.sentiment_counts.neutral ?? 0,
    positive: dto.sentiment_counts.positive ?? 0,
    mixed: dto.sentiment_counts.mixed ?? 0,
  },
  skippedByFilter: dto.skipped_by_filter ?? 0,
  skippedEmpty: dto.skipped_empty ?? 0,
  totalCompletionTokens: dto.total_completion_tokens ?? 0,
  totalPromptTokens: dto.total_prompt_tokens ?? 0,
});

export const mapCleanJobFormToRequest = (
  values: CleanJobFormValues,
): CleanJobPayload => {
  return compactObject({
    text_columns: values.textColumns
      ?.map((v) => v.value.trim())
      .filter(Boolean),
    output_path: values.outputPath?.trim(),
    clean_options: {
      remove_english: values.cleanOptions.removeEnglish,
      remove_numbers: values.cleanOptions.removeNumbers,
      remove_special: values.cleanOptions.removeSpecial,
      remove_monosyllables: values.cleanOptions.removeMonosyllables,
    },
    force: values.force,
  });
};
