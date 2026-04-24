from __future__ import annotations

from typing import Any

from ._pydantic_compat import BaseModel, Field


class DateRangeModel(BaseModel):
    start: str | None = None
    end: str | None = None


class RepresentativeSampleModel(BaseModel):
    text: str
    source_index: int | None = None
    row_id: str | None = None
    chunk_id: str | None = None


class CoverageModel(BaseModel):
    documents_considered: int
    total_documents: int


class RankedIssueModel(BaseModel):
    rank: int
    label: str
    count: int
    representative_samples: list[RepresentativeSampleModel] = Field(default_factory=list)
    date_range: DateRangeModel = Field(default_factory=lambda: DateRangeModel.model_validate({}))


class RankedIssueSummaryArtifactModel(BaseModel):
    ranked_issues: list[RankedIssueModel] = Field(default_factory=list)
    coverage: CoverageModel
    result_scope: str
    quality_tier: str
    llm_output_parsed_strictly: bool | None = None


class EvidenceItemModel(BaseModel):
    rank: int
    source_index: int | None = None
    snippet: str
    rationale: str
    row_id: str | None = None
    chunk_id: str | None = None
    chunk_ref: str | None = None
    chunk_format: str | None = None
    chunk_index: int | None = None
    char_start: int | None = None
    char_end: int | None = None


class IssueEvidenceArtifactModel(BaseModel):
    selection_source: str
    citation_mode: str
    analysis_context: list[dict[str, Any]] = Field(default_factory=list)
    summary: str
    key_findings: list[str] = Field(default_factory=list)
    evidence: list[EvidenceItemModel] = Field(default_factory=list)
    follow_up_questions: list[str] = Field(default_factory=list)
    result_scope: str
    quality_tier: str
    llm_output_parsed_strictly: bool


class ExecutionFinalAnswerModel(BaseModel):
    schema_version: str
    status: str
    generation_mode: str
    headline: str
    answer_text: str
    key_points: list[str] = Field(default_factory=list)
    caveats: list[str] = Field(default_factory=list)
    evidence: list[EvidenceItemModel] = Field(default_factory=list)
    follow_up_questions: list[str] = Field(default_factory=list)
    model: str
    generated_at: str
    result_scope: str
    quality_tier: str
    llm_output_parsed_strictly: bool


class IssueOverviewViewModel(BaseModel):
    view_name: str
    ranked_issues: list[RankedIssueModel] = Field(default_factory=list)
    coverage: CoverageModel
    summary: str
    key_findings: list[str] = Field(default_factory=list)
    evidence: list[EvidenceItemModel] = Field(default_factory=list)
    selection_source: str
    quality_tier: str
    llm_output_parsed_strictly: bool


class ClusterLabelCandidateModel(BaseModel):
    cluster_id: str
    label: str
    candidate_labels: list[str] = Field(default_factory=list)


class ClusterOverviewViewModel(BaseModel):
    view_name: str
    ranked_issues: list[RankedIssueModel] = Field(default_factory=list)
    coverage: CoverageModel
    summary: str
    key_findings: list[str] = Field(default_factory=list)
    evidence: list[EvidenceItemModel] = Field(default_factory=list)
    cluster_labels: list[ClusterLabelCandidateModel] = Field(default_factory=list)
    selection_source: str
    quality_tier: str
    llm_output_parsed_strictly: bool


def validate_ranked_issue_summary_artifact(artifact: dict[str, Any]) -> RankedIssueSummaryArtifactModel:
    return RankedIssueSummaryArtifactModel.model_validate(artifact)


def validate_issue_evidence_artifact(artifact: dict[str, Any]) -> IssueEvidenceArtifactModel:
    return IssueEvidenceArtifactModel.model_validate(artifact)


def validate_execution_final_answer(answer: dict[str, Any]) -> ExecutionFinalAnswerModel:
    return ExecutionFinalAnswerModel.model_validate(answer)


def validate_issue_overview_view(view: dict[str, Any]) -> IssueOverviewViewModel:
    return IssueOverviewViewModel.model_validate(view)


def validate_cluster_overview_view(view: dict[str, Any]) -> ClusterOverviewViewModel:
    return ClusterOverviewViewModel.model_validate(view)


__all__ = [
    "ClusterLabelCandidateModel",
    "ClusterOverviewViewModel",
    "CoverageModel",
    "DateRangeModel",
    "EvidenceItemModel",
    "ExecutionFinalAnswerModel",
    "IssueEvidenceArtifactModel",
    "IssueOverviewViewModel",
    "RankedIssueModel",
    "RankedIssueSummaryArtifactModel",
    "RepresentativeSampleModel",
    "validate_cluster_overview_view",
    "validate_execution_final_answer",
    "validate_issue_evidence_artifact",
    "validate_issue_overview_view",
    "validate_ranked_issue_summary_artifact",
]
