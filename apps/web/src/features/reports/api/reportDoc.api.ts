import { apiClient } from "@/api/client";
import type {
  ReportCreateRequestDto,
  ReportDto,
  ReportFromTemplateRequestDto,
  ReportFromTemplateResponseDto,
  ReportItemAppendRequestDto,
  ReportItemAppendResponseDto,
  ReportListResponseDto,
  ReportUpdateRequestDto,
} from "../models";

// 보고서 문서(Report) CRUD. blocks는 opaque snapshot으로 그대로 영속한다.
export const reportDocApi = {
  // GET 목록 (project 스코프, 최신순). blocks 본문 제외 경량 summary.
  list: (projectId: string) =>
    apiClient
      .get<ReportListResponseDto>(`/projects/${projectId}/reports`)
      .then((r) => r.data),

  // GET 단건 (blocks 포함).
  get: (projectId: string, reportId: string) =>
    apiClient
      .get<ReportDto>(`/projects/${projectId}/reports/${reportId}`)
      .then((r) => r.data),

  // POST 생성. 미지정 시 빈 보고서.
  create: (projectId: string, body: ReportCreateRequestDto) =>
    apiClient
      .post<ReportDto>(`/projects/${projectId}/reports`, body)
      .then((r) => r.data),

  // POST 기본 템플릿 생성. dataset_id의 active version으로 데이터 기초 분석 보고서를 만든다.
  fromTemplate: (projectId: string, body: ReportFromTemplateRequestDto) =>
    apiClient
      .post<ReportFromTemplateResponseDto>(
        `/projects/${projectId}/reports/from_template`,
        body,
      )
      .then((r) => r.data),

  // POST item append. 기존 보고서 blocks 뒤에 분석 결과(run_id) 1개를 추가한다.
  appendItem: (
    projectId: string,
    reportId: string,
    body: ReportItemAppendRequestDto,
  ) =>
    apiClient
      .post<ReportItemAppendResponseDto>(
        `/projects/${projectId}/reports/${reportId}/item`,
        body,
      )
      .then((r) => r.data),

  // PUT 전체 갱신 (title + blocks 교체).
  update: (projectId: string, reportId: string, body: ReportUpdateRequestDto) =>
    apiClient
      .put<ReportDto>(`/projects/${projectId}/reports/${reportId}`, body)
      .then((r) => r.data),

  // DELETE 문서.
  remove: (projectId: string, reportId: string) =>
    apiClient
      .delete(`/projects/${projectId}/reports/${reportId}`)
      .then(() => undefined),
};
