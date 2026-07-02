package service

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"analysis-support-platform/control-plane/internal/domain"
	"analysis-support-platform/control-plane/internal/id"
	"analysis-support-platform/control-plane/internal/registry"
	"analysis-support-platform/control-plane/internal/store"
)

// CreateReportFromTemplate — 기본 템플릿으로 보고서를 생성한다(ADR: 데이터 기초 분석 보고서).
// clean ready인 dataset_version만 대상. 각 섹션은 required_build가 ready일 때만 블록으로
// 채워지고, 아니면 missing_sections에 기록한다. 템플릿 조립 자체는 빌드 때 계산된 summary를
// reshape할 뿐이지만, 일부 섹션(loadChannelGenuineBreakdown/loadRecentYearStats)은
// on-demand로 DuckDB 집계를 수행한다(ADR-024 경계 부채 — 후속 worker 이동 대상).
// 생성된 블록엔 데이터 스냅샷 + source binding을 함께 저장(나중에 "갱신" 용).
// 계약: docs/api/report_basic_template.sample.md
func (s *DatasetService) CreateReportFromTemplate(projectID string, req domain.ReportFromTemplateRequest) (domain.ReportFromTemplateResponse, error) {
	// dataset_id → 그 dataset의 active version으로 해석(analyze 흐름과 동일 패턴).
	datasetID := strings.TrimSpace(req.DatasetID)
	if datasetID == "" {
		return domain.ReportFromTemplateResponse{}, ErrInvalidArgument{Message: "dataset_id is required"}
	}
	dataset, err := s.GetDataset(projectID, datasetID)
	if err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	versionID := ""
	if dataset.ActiveDatasetVersionID != nil {
		versionID = strings.TrimSpace(*dataset.ActiveDatasetVersionID)
	}
	if versionID == "" {
		return domain.ReportFromTemplateResponse{}, ErrInvalidArgument{
			Message: "dataset has no active version — set an active version first",
		}
	}
	built, err := s.buildReportTemplateBlocks(projectID, req.TemplateID, versionID)
	if err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}

	blocksJSON, err := json.Marshal(built.blocks)
	if err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	now := time.Now().UTC()
	report := domain.Report{
		ReportID:         id.New(),
		ProjectID:        projectID,
		Title:            built.title,
		DatasetVersionID: built.versionID,
		Blocks:           blocksJSON,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if err := s.store.CreateReport(report); err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	saved, err := s.store.GetReport(projectID, report.ReportID)
	if err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	return domain.ReportFromTemplateResponse{
		Report:           saved,
		IncludedSections: built.included,
		MissingSections:  built.missing,
	}, nil
}

// GetBasicAnalysis — 데이터셋 버전 "기초분석보고서" 탭용 read-only 조회. report를 저장하지
// 않고 템플릿 블록만 즉석 reshape해서 반환한다(CreateReportFromTemplate과 블록 생성 로직 공유).
// templateID가 비면 기본 템플릿(unstructured_basic_v1)을 쓴다.
func (s *DatasetService) GetBasicAnalysis(projectID, versionID, templateID string) (domain.ReportBasicAnalysisResponse, error) {
	if strings.TrimSpace(templateID) == "" {
		templateID = defaultBasicReportTemplateID
	}
	built, err := s.buildReportTemplateBlocks(projectID, templateID, versionID)
	if err != nil {
		return domain.ReportBasicAnalysisResponse{}, err
	}
	return domain.ReportBasicAnalysisResponse{
		TemplateID:       built.templateID,
		DatasetVersionID: built.versionID,
		Title:            built.title,
		Blocks:           built.blocks,
		IncludedSections: built.included,
		MissingSections:  built.missing,
	}, nil
}

// defaultBasicReportTemplateID — 기초분석보고서 탭이 template_id를 안 주면 쓰는 기본 템플릿.
// (현재 비정형 SNS 후기 1종. data_type별 기본 선택은 후속.)
const defaultBasicReportTemplateID = "unstructured_basic_v1"

// reportTemplateBuild — 템플릿 블록 생성 결과(저장 전). GET 미리보기와 POST 저장이 공유.
type reportTemplateBuild struct {
	templateID string
	versionID  string
	title      string
	blocks     []map[string]any
	included   []string
	missing    []domain.ReportMissingSection
}

// buildReportTemplateBlocks — 템플릿 섹션을 순회해 ready인 build만 블록으로 reshape한다.
// 대부분은 빌드 때 계산된 summary 재구성이지만, channel_distribution/recent_year 섹션은
// 하위 load*가 on-demand DuckDB 집계를 한다(ADR-024 부채 — worker 이동 대상). 저장하지 않는다.
func (s *DatasetService) buildReportTemplateBlocks(projectID, templateID, versionID string) (reportTemplateBuild, error) {
	if err := s.requireProject(projectID); err != nil {
		return reportTemplateBuild{}, err
	}
	template, ok := registry.ReportTemplateByID(strings.TrimSpace(templateID))
	if !ok {
		return reportTemplateBuild{}, ErrInvalidArgument{Message: "unknown template_id"}
	}
	versionID = strings.TrimSpace(versionID)
	if versionID == "" {
		return reportTemplateBuild{}, ErrInvalidArgument{Message: "dataset_version_id is required"}
	}

	rawVersion, err := s.store.GetDatasetVersion(projectID, versionID)
	if err != nil {
		if err == store.ErrNotFound {
			return reportTemplateBuild{}, ErrNotFound{Resource: "dataset version"}
		}
		return reportTemplateBuild{}, err
	}
	version, err := s.GetDatasetVersion(projectID, rawVersion.DatasetID, versionID)
	if err != nil {
		return reportTemplateBuild{}, err
	}
	if !reportCleanReady(version) {
		return reportTemplateBuild{}, ErrInvalidArgument{Message: "clean_not_ready"}
	}

	labels := s.newReportLabels(version)
	roots := map[string]reportBuildRoot{} // (build|dateFilter) → (root, ready) 캐시

	// 최신년도 라벨용(silverone 2026-06-25) — date_filter 섹션에 "YYYY년 기준" 배지.
	// 날짜 없으면 필터 미적용이라 라벨도 안 단다. 보고서당 1회 계산.
	recentYear, recentYearOK := s.reportRecentYear(version)

	blocks := make([]map[string]any, 0, len(template.Sections))
	included := make([]string, 0, len(template.Sections))
	missing := make([]domain.ReportMissingSection, 0)

	for _, section := range template.Sections {
		root := s.reportBuildRoot(version, section.RequiredBuild, section.DateFilter, roots)
		if !root.ready {
			missing = append(missing, domain.ReportMissingSection{
				SectionID: section.ID,
				Reason:    strings.TrimSpace(section.RequiredBuild) + "_not_ready",
			})
			continue
		}
		block := s.buildReportBlock(version, section, roots, labels)
		// 개요 외 섹션(date_filter=recent_year)은 최신년도 데이터만 집계됨을 명시.
		if section.DateFilter == "recent_year" && recentYearOK {
			block["scope_label"] = fmt.Sprintf("%d년 기준", recentYear)
		}
		blocks = append(blocks, block)
		included = append(included, section.ID)
	}

	title := strings.TrimSpace(template.ReportTitle)
	if title == "" {
		title = "데이터 기초 분석 보고서"
	}
	return reportTemplateBuild{
		templateID: template.TemplateID,
		versionID:  versionID,
		title:      title,
		blocks:     blocks,
		included:   included,
		missing:    missing,
	}, nil
}

// ── build summary 루트 (캐시) ──────────────────────────────────────────────

type reportBuildRoot struct {
	root  map[string]any
	ready bool
}

func (s *DatasetService) reportBuildRoot(version domain.DatasetVersion, build, dateFilter string, cache map[string]reportBuildRoot) reportBuildRoot {
	build = strings.TrimSpace(build)
	dateFilter = strings.TrimSpace(dateFilter)
	// 같은 build을 전체/최신년도 두 스코프로 안전하게 공존시키기 위해 캐시 키에 필터 포함.
	key := build + "|" + dateFilter
	if cached, ok := cache[key]; ok {
		return cached
	}
	result := s.loadReportBuildRoot(version, build, dateFilter)
	cache[key] = result
	return result
}

func (s *DatasetService) loadReportBuildRoot(version domain.DatasetVersion, build, dateFilter string) reportBuildRoot {
	// date_filter="recent_year" 섹션은 개요 외 분포를 최신년도 doc로 좁힌다(clause_label/
	// clause_keywords/channel_breakdown만 대상. version/clean/doc_genuineness는 항상 전체).
	recentYear := dateFilter == "recent_year"
	switch build {
	case "version":
		datasetName := ""
		var dataset domain.Dataset
		if ds, err := s.GetDataset(version.ProjectID, version.DatasetID); err == nil {
			dataset = ds
			datasetName = ds.Name
		}
		cleanRef := reportArtifactRef(version.Metadata, "clean_uri", "cleaned_ref")
		if cleanRef == "" && version.CleanURI != nil {
			cleanRef = strings.TrimSpace(*version.CleanURI)
		}
		period := metadataString(version.Metadata, "data_period", "")
		if period == "" && reportCleanReady(version) && cleanRef != "" {
			if lo, hi, err := loadDataPeriod(cleanRef); err == nil {
				period = formatDataPeriodRange(lo, hi) // 원본 데이터의 게시일 범위
			}
		}
		model := metadataString(version.Metadata, "clause_label_model", metadataString(version.Metadata, "doc_genuineness_model", ""))
		if model == "" {
			model = s.reportPreprocessModels(version.Metadata) // 전처리에 쓴 LLOA 모델(config label)
		}
		// #31 분석 개요 — 분석 대상/기간/수집 채널/수집 키워드/유형 정의.
		// 축제 메타는 프로젝트 레벨(project.metadata.festival)에서 조회한다(2026-07-01).
		festName, festPeriods := "", []map[string]any(nil)
		if project, err := s.store.GetProject(version.ProjectID); err == nil {
			festName, festPeriods = festivalMeta(project)
		}
		subject := festName
		if subject == "" {
			subject = datasetSubjectName(dataset) // metadata.doc_genuineness.subject_name
		}
		if subject == "" {
			subject = datasetName
		}
		return reportBuildRoot{root: map[string]any{
			"dataset_name":  datasetName,
			"version_label": metadataString(version.Metadata, "version_label", ""),
			"data_period":   period,
			"lloa_model":    model,
			// #31
			"analysis_subject":    subject,
			"analysis_periods":    analysisPeriodsView(festPeriods),
			"collection_channels": loadCollectionValues(cleanRef, collectionChannelCandidates, false),
			"collection_keywords": loadCollectionValues(cleanRef, collectionKeywordCandidates, true),
			"type_definitions":    loadTypeDefinitions(resolveReportTaxonomyID(version, dataset)),
		}, ready: true}
	case "clean":
		if !reportCleanReady(version) {
			return reportBuildRoot{}
		}
		summary := map[string]any{}
		if version.CleanSummary != nil {
			summary = cleanSummaryToMap(version.CleanSummary)
		} else if m, ok := version.Metadata["clean_summary"].(map[string]any); ok {
			summary = m
		}
		return reportBuildRoot{root: map[string]any{"summary": summary}, ready: true}
	case "clause_label":
		ref := reportArtifactRef(version.Metadata, "clause_label_ref", "clause_label_uri")
		if ref == "" {
			return reportBuildRoot{}
		}
		// 최신년도 필터(recentYear) — clean에 doc_id JOIN해 최신년도 절만 집계. 날짜 없으면 no-op.
		filters := recentYearFilters(version, recentYear)
		// ADR-028 verify 모드는 schema가 달라 전용 로더(final_label 기준 집계). view
		// 핸들러(dataset_artifact_views.go)와 동일 분기.
		var summary map[string]any
		var err error
		if metadataString(version.Metadata, "clause_label_mode", "") == "verify" {
			summary, _, _, _, err = loadClauseLabelVerifyArtifact(ref, "", 1, 0, "", "", false, false, filters...)
		} else {
			summary, _, _, _, err = loadClauseLabelArtifact(ref, "", 1, 0, "", "", filters...)
		}
		if err != nil {
			return reportBuildRoot{}
		}
		return reportBuildRoot{root: map[string]any{"summary": summary}, ready: true}
	case "doc_genuineness":
		ref := reportArtifactRef(version.Metadata, "doc_genuineness_ref", "doc_genuineness_uri")
		if ref == "" {
			return reportBuildRoot{}
		}
		cleanRef := reportArtifactRef(version.Metadata, "clean_uri", "cleaned_ref")
		if cleanRef == "" && version.CleanURI != nil {
			cleanRef = strings.TrimSpace(*version.CleanURI)
		}
		// ADR-026 verify 모드는 schema가 달라(final_label 등) 전용 로더를 쓴다.
		// view 핸들러(dataset_artifact_views.go)와 동일 분기.
		var summary map[string]any
		var err error
		if metadataString(version.Metadata, "doc_genuineness_mode", "") == "verify" {
			summary, _, _, _, err = loadDocGenuinenessVerifyArtifact(ref, cleanRef, 1, 0, "", false, false)
		} else {
			summary, _, _, _, err = loadDocGenuinenessArtifact(ref, cleanRef, 1, 0, version.DatasetVersionID, "")
		}
		if err != nil {
			return reportBuildRoot{}
		}
		return reportBuildRoot{root: map[string]any{"summary": summary}, ready: true}
	case "clause_keywords":
		ref := reportArtifactRef(version.Metadata, "clause_keywords_ref", "clause_keywords_uri")
		if ref == "" {
			return reportBuildRoot{}
		}
		// 키워드 정제 사전(#24) — 보고서도 키워드 뷰와 동일하게 활성 block/synonym을
		// 조회 overlay로 적용한다. 제외/병합이 재빌드 없이 보고서(감성별 상위 키워드)에
		// 즉시 반영된다(옛 Phase 1은 nil로 미적용했음). 규칙 조회 실패는 미적용(nil) fallback.
		// 최신년도 필터(recentYear) — clean에 doc_id JOIN해 최신년도 키워드만 집계.
		rules, _ := s.store.ListKeywordDictionaryRules(version.ProjectID, version.DatasetID, true)
		summary, _, _, err := loadClauseKeywordsArtifact(ref, 1, 0, "", "", "", "", rules, recentYearFilters(version, recentYear)...)
		if err != nil {
			return reportBuildRoot{}
		}
		return reportBuildRoot{root: map[string]any{"summary": summary}, ready: true}
	case "channel_breakdown":
		// 별도 build 단계가 아니다. clean(source_json에 원본 채널 컬럼 보존)과
		// doc_genuineness를 doc_id로 JOIN해 "채널별 진성 문서 수"를 즉석 집계한다.
		// clean + doc_genuineness 둘 다 ready여야 한다.
		cleanRef := reportArtifactRef(version.Metadata, "clean_uri", "cleaned_ref")
		if cleanRef == "" && version.CleanURI != nil {
			cleanRef = strings.TrimSpace(*version.CleanURI)
		}
		genRef := reportArtifactRef(version.Metadata, "doc_genuineness_ref", "doc_genuineness_uri")
		if cleanRef == "" || genRef == "" {
			return reportBuildRoot{}
		}
		verify := metadataString(version.Metadata, "doc_genuineness_mode", "") == "verify"
		summary, err := loadChannelGenuineBreakdown(cleanRef, genRef, verify, recentYear)
		if err != nil || summary == nil {
			return reportBuildRoot{}
		}
		return reportBuildRoot{root: map[string]any{"summary": summary}, ready: true}
	case "recent_year_stats":
		// 문서 개요 "최근 연도 기준" item용. clean 날짜의 max 연도로 진성수/절수를
		// 즉석 집계. clean + doc_genuineness + clause_label 셋 다 + 날짜 필요.
		cleanRef := reportArtifactRef(version.Metadata, "clean_uri", "cleaned_ref")
		if cleanRef == "" && version.CleanURI != nil {
			cleanRef = strings.TrimSpace(*version.CleanURI)
		}
		genRef := reportArtifactRef(version.Metadata, "doc_genuineness_ref", "doc_genuineness_uri")
		// clause_label은 optional(절 수만 거기 의존). clean + doc_genuineness만 필수.
		clauseRef := reportArtifactRef(version.Metadata, "clause_label_ref", "clause_label_uri")
		if cleanRef == "" || genRef == "" {
			return reportBuildRoot{}
		}
		genVerify := metadataString(version.Metadata, "doc_genuineness_mode", "") == "verify"
		summary, err := loadRecentYearStats(cleanRef, genRef, clauseRef, genVerify)
		if err != nil || summary == nil {
			return reportBuildRoot{}
		}
		return reportBuildRoot{root: summary, ready: true}
	default:
		return reportBuildRoot{}
	}
}

// loadChannelGenuineBreakdown — cleaned.parquet(clean이 source_json에 원본 row 전체를
// 보존)와 doc_genuineness artifact를 doc_id로 JOIN해 "채널별 진성(genuine_review) 문서
// 수"를 즉석 집계한다. 별도 build 단계 없이 on-demand reshape(보고서 철학).
//
// 채널 컬럼명은 데이터셋마다 다르므로(수집채널/채널/channel/source/…) 후보 중 값이 가장
// 많이 채워진 컬럼을 자동 선택한다. 채널 데이터가 전혀 없으면 nil을 반환해 섹션을
// missing_sections로 떨어뜨린다. verify 모드는 진성 라벨 컬럼이 final_label, 단일 모드는
// genuineness다(view 핸들러와 동일).
func loadChannelGenuineBreakdown(cleanRef, genuinenessRef string, verifyMode bool, recentYear bool) (map[string]any, error) {
	if strings.TrimSpace(cleanRef) == "" || strings.TrimSpace(genuinenessRef) == "" {
		return nil, nil
	}
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	cleanSrc := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
	genSrc := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(genuinenessRef))

	// 최신년도 섹션 필터(silverone 2026-06-25) — 채널 doc 집합을 최신년도로 제한.
	// 채널 subselect가 clean에서 직접 읽으므로 연도 술어를 인라인으로 건다. 날짜 없으면 no-op.
	docFilter := ""
	if recentYear {
		if pred, _, ok := recentYearPredicate(db, cleanSrc); ok {
			docFilter = " AND " + pred
		}
	}

	// 원본 채널 컬럼 후보 (코드 상수 — SQL injection 위험 없음).
	channelExpr := func(field string) string {
		return fmt.Sprintf(`json_extract_string(source_json, '$."%s"')`, field)
	}
	candidates := []string{"수집채널", "채널", "channel", "source", "platform", "매체", "미디어"}
	chosen := ""
	best := -1
	for _, f := range candidates {
		expr := channelExpr(f)
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL AND %s <> ''", cleanSrc, expr, expr)
		var cnt int
		if err := db.QueryRow(q).Scan(&cnt); err != nil {
			continue // 컬럼/스키마 미스는 후보 skip
		}
		if cnt > best {
			best = cnt
			chosen = f
		}
	}
	if chosen == "" || best <= 0 {
		return nil, nil // 채널 데이터 없음 → not ready
	}

	labelCol := "genuineness"
	if verifyMode {
		labelCol = "final_label"
	}
	q := fmt.Sprintf(`
		SELECT c.channel, COUNT(*) AS cnt FROM
			(SELECT doc_id, %s AS channel FROM %s WHERE TRUE%s) c
			JOIN (SELECT doc_id FROM %s WHERE %s = 'genuine_review') g ON c.doc_id = g.doc_id
		WHERE c.channel IS NOT NULL AND c.channel <> ''
		GROUP BY c.channel`,
		channelExpr(chosen), cleanSrc, docFilter, genSrc, labelCol)
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	channels := map[string]int{}
	total := 0
	for rows.Next() {
		var key string
		var cnt int
		if err := rows.Scan(&key, &cnt); err != nil {
			return nil, err
		}
		channels[key] += cnt
		total += cnt
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"total": total, "channels": channels, "channel_field": chosen}, nil
}

// reportPreprocessModels — 분석 개요의 "분석 모델"용. 전처리(doc_genuineness/clause_label)
// 빌드 당시 metadata에 snapshot된 LLOA 모델 id를 모아 고유 목록 문자열로. verify 모드는
// applied.classify_models(2개), 단일 모드는 model 1개. 표시는 raw id가 아니라 모델 config
// (config/lloa_models.json)의 label로 변환한다(예: wisenut/wise-lloa-max-v1.2.1 → LLOA Max
// 1.2.1). config에 없는 모델은 "wisenut/" prefix만 떼서 fallback.
func (s *DatasetService) reportPreprocessModels(metadata map[string]any) string {
	labelByID := map[string]string{}
	for _, opt := range s.LLOAModelOptions() {
		labelByID[opt.ModelID] = opt.Label
	}

	seen := map[string]bool{}
	order := make([]string, 0, 4)
	add := func(rawID string) {
		rawID = strings.TrimSpace(rawID)
		if rawID == "" {
			return
		}
		display := labelByID[rawID]
		if display == "" {
			display = rawID
			if i := strings.LastIndex(display, "/"); i >= 0 {
				display = display[i+1:]
			}
		}
		if seen[display] {
			return
		}
		seen[display] = true
		order = append(order, display)
	}
	for _, sk := range []string{"doc_genuineness_summary", "clause_label_summary"} {
		if applied, ok := summaryMetadataMap(metadata, sk, "applied"); ok {
			if cm, ok := asList(applied["classify_models"]); ok {
				for _, v := range cm {
					if str, ok := v.(string); ok {
						add(str)
					}
				}
			}
		}
		add(summaryMetadataString(metadata, sk, "model"))
	}
	return strings.Join(order, ", ")
}

// formatDataPeriodRange — 원본 게시일 min/max를 "lo ~ hi"(또는 단일 lo)로. 분석 개요의
// "분석 기간" 문자열에 쓴다. lo가 없으면 빈 문자열. min/max 산출은 loadDataPeriod가
// clean source_json의 날짜 컬럼(게시일/작성일/…) 후보 자동선택 + TRY_CAST로 담당한다.
func formatDataPeriodRange(lo, hi string) string {
	if strings.TrimSpace(lo) == "" {
		return ""
	}
	if strings.TrimSpace(hi) == "" || hi == lo {
		return lo
	}
	return lo + " ~ " + hi
}

// reportDateColumnCandidates — clean source_json에서 날짜로 쓸 원본 컬럼 후보(한/영).
// created_at(date_column 정규화) 미선택 데이터의 fallback 추론용.
var reportDateColumnCandidates = []string{
	"게시일", "작성일", "작성시간", "등록일", "수집일", "날짜",
	"docDatetime", "doc_datetime", "pub_date", "reg_date", "post_date",
	"write_date", "regdate", "datetime", "date", "published_at",
}

// reportCleanDateExpr — cleaned.parquet에서 날짜로 쓸 SQL DATE 표현식을 고른다.
// created_at(date_column 정규화) 우선, 없으면 source_json 원본 날짜 컬럼 후보 중 값이
// 채워진 첫 컬럼. 유효한 게 없으면 "". 분석기간/최근연도 집계가 공유한다.
func reportCleanDateExpr(db *sql.DB, cleanSrc string) string {
	var cnt int
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(TRY_CAST(created_at AS DATE)) FROM %s", cleanSrc)).Scan(&cnt); err == nil && cnt > 0 {
		return "TRY_CAST(created_at AS DATE)"
	}
	for _, f := range reportDateColumnCandidates {
		expr := fmt.Sprintf(`TRY_CAST(json_extract_string(source_json, '$."%s"') AS DATE)`, f)
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(%s) FROM %s", expr, cleanSrc)).Scan(&cnt); err == nil && cnt > 0 {
			return expr
		}
	}
	return ""
}

func loadDataPeriod(cleanRef string) (string, string, error) {
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return "", "", err
	}
	defer cleanup()

	cleanSrc := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
	dateExpr := reportCleanDateExpr(db, cleanSrc)
	if dateExpr == "" {
		return "", "", nil
	}
	q := fmt.Sprintf("SELECT CAST(MIN(%s) AS VARCHAR), CAST(MAX(%s) AS VARCHAR) FROM %s", dateExpr, dateExpr, cleanSrc)
	var lo, hi sql.NullString
	if err := db.QueryRow(q).Scan(&lo, &hi); err != nil {
		return "", "", err
	}
	if !lo.Valid {
		return "", "", nil
	}
	return lo.String, hi.String, nil
}

// loadRecentYearStats — 문서 개요의 "최근 연도 기준" 집계. clean 날짜에서 가장 최근
// 연도를 구해, 그 연도 문서의 진성(genuine_review) 문서수와 절 수를 doc_id JOIN으로 센다.
// clean + doc_genuineness + 날짜가 필수(진성 문서수 최근연도). clause_label은 optional —
// 있으면 절 수(최근연도)도 채우고, 없으면 진성 문서수만 반환한다.
func loadRecentYearStats(cleanRef, genRef, clauseRef string, genVerify bool) (map[string]any, error) {
	if strings.TrimSpace(cleanRef) == "" || strings.TrimSpace(genRef) == "" {
		return nil, nil
	}
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil, err
	}
	defer cleanup()

	cleanSrc := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
	dateExpr := reportCleanDateExpr(db, cleanSrc)
	if dateExpr == "" {
		return nil, nil // 날짜 없으면 최근 연도 집계 불가
	}

	var maxYear sql.NullInt64
	if err := db.QueryRow(fmt.Sprintf("SELECT MAX(EXTRACT(year FROM %s)) FROM %s", dateExpr, cleanSrc)).Scan(&maxYear); err != nil {
		return nil, err
	}
	if !maxYear.Valid {
		return nil, nil
	}
	year := int(maxYear.Int64)

	// 최근 연도 문서(row_id). doc_genuineness/clause_label의 doc_id와 매핑된다.
	recentDocs := fmt.Sprintf("(SELECT row_id FROM %s WHERE EXTRACT(year FROM %s) = %d)", cleanSrc, dateExpr, year)

	genSrc := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(genRef))
	genLabelCol := "genuineness"
	if genVerify {
		genLabelCol = "final_label"
	}
	var genuineDocs int
	if err := db.QueryRow(fmt.Sprintf(
		"SELECT COUNT(*) FROM %s AS g JOIN %s AS r ON g.doc_id = r.row_id WHERE g.%s = 'genuine_review'",
		genSrc, recentDocs, genLabelCol,
	)).Scan(&genuineDocs); err != nil {
		return nil, err
	}

	result := map[string]any{
		"recent_year":  year,
		"year_label":   fmt.Sprintf("%d년", year),
		"genuine_docs": genuineDocs,
	}

	// clause_label은 optional — 빌드돼 있으면 절 수(최근연도)도 채운다.
	if strings.TrimSpace(clauseRef) != "" {
		clauseSrc := fmt.Sprintf("read_json('%s', format='newline_delimited')", escapeDuckDBLiteral(clauseRef))
		var clauseCount int
		if err := db.QueryRow(fmt.Sprintf(
			"SELECT COUNT(*) FROM %s AS c JOIN %s AS r ON c.doc_id = r.row_id",
			clauseSrc, recentDocs,
		)).Scan(&clauseCount); err != nil {
			return nil, err
		}
		result["clause_count"] = clauseCount
	}

	return result, nil
}

// ── 최신년도 섹션 필터 (silverone 2026-06-25) ──────────────────────────────
// 기초보고서의 개요 2섹션(분석/문서)을 뺀 나머지 섹션은 "최신년도 데이터로만" 집계한다.
// clause_label/clause_keywords artifact엔 날짜가 없어 clean(created_at/원본 날짜)에 doc_id
// (=row_id) JOIN으로 최신년도(MAX year)를 거른다. loadRecentYearStats와 동일 패턴.

// artifactRecentYearFilter — 공유 집계 로더에 trailing 가변인자로 넘기는 최신년도 필터.
// cleanRef가 있고 clean 날짜가 유효하면 로더가 source를 최신년도 doc로 좁힌다(없으면 no-op).
type artifactRecentYearFilter struct {
	cleanRef string
}

// firstRecentYearFilter — variadic 중 유효한(cleanRef 있는) 첫 필터를 돌려준다.
func firstRecentYearFilter(filters []artifactRecentYearFilter) (artifactRecentYearFilter, bool) {
	if len(filters) > 0 && strings.TrimSpace(filters[0].cleanRef) != "" {
		return filters[0], true
	}
	return artifactRecentYearFilter{}, false
}

// recentYearPredicate — clean source의 "최신연도(MAX year)" SQL 술어(EXTRACT(year …)=max)와
// 연도를 돌려준다. 날짜 컬럼이 없거나 연도를 못 구하면 ("", 0, false) → 호출부는 전체 fallback.
func recentYearPredicate(db *sql.DB, cleanSrc string) (string, int, bool) {
	dateExpr := reportCleanDateExpr(db, cleanSrc)
	if dateExpr == "" {
		return "", 0, false
	}
	var maxYear sql.NullInt64
	if err := db.QueryRow(fmt.Sprintf("SELECT MAX(EXTRACT(year FROM %s)) FROM %s", dateExpr, cleanSrc)).Scan(&maxYear); err != nil || !maxYear.Valid {
		return "", 0, false
	}
	year := int(maxYear.Int64)
	return fmt.Sprintf("EXTRACT(year FROM %s) = %d", dateExpr, year), year, true
}

// recentYearDocSubquery — clean source에서 최신연도 row_id 서브쿼리. clause_label/clause_keywords
// artifact의 doc_id(=clean row_id)를 좁히는 데 쓴다. 날짜 없으면 ("", 0, false).
func recentYearDocSubquery(db *sql.DB, cleanSrc string) (string, int, bool) {
	pred, year, ok := recentYearPredicate(db, cleanSrc)
	if !ok {
		return "", 0, false
	}
	return fmt.Sprintf("(SELECT row_id FROM %s WHERE %s)", cleanSrc, pred), year, true
}

// wrapSourceRecentYear — source(읽기 식)를 최신년도 doc로 좁힌다. filter 미적용/날짜 없으면
// 원본 source 그대로 반환(두 번째 반환값 false). 모든 하위 집계가 이 source 위에서 돌아간다.
func wrapSourceRecentYear(db *sql.DB, source string, filters []artifactRecentYearFilter) (string, bool) {
	filter, ok := firstRecentYearFilter(filters)
	if !ok {
		return source, false
	}
	cleanSrc := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(filter.cleanRef))
	sub, _, valid := recentYearDocSubquery(db, cleanSrc)
	if !valid {
		return source, false
	}
	return fmt.Sprintf("(SELECT * FROM %s WHERE doc_id IN %s)", source, sub), true
}

// recentYearFilters — recentYear일 때 clean ref를 담은 로더 필터를 만든다(아니면 nil).
// clean ref가 없으면 nil(전체). loadClauseLabelArtifact/loadClauseKeywordsArtifact의 trailing
// 가변인자로 전달한다.
func recentYearFilters(version domain.DatasetVersion, recentYear bool) []artifactRecentYearFilter {
	if !recentYear {
		return nil
	}
	cleanRef := reportArtifactRef(version.Metadata, "clean_uri", "cleaned_ref")
	if cleanRef == "" && version.CleanURI != nil {
		cleanRef = strings.TrimSpace(*version.CleanURI)
	}
	if cleanRef == "" {
		return nil
	}
	return []artifactRecentYearFilter{{cleanRef: cleanRef}}
}

// reportRecentYear — 보고서 섹션 라벨("2025년 기준")용. clean 날짜의 MAX 연도. 날짜 없으면 false.
func (s *DatasetService) reportRecentYear(version domain.DatasetVersion) (int, bool) {
	cleanRef := reportArtifactRef(version.Metadata, "clean_uri", "cleaned_ref")
	if cleanRef == "" && version.CleanURI != nil {
		cleanRef = strings.TrimSpace(*version.CleanURI)
	}
	if cleanRef == "" {
		return 0, false
	}
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return 0, false
	}
	defer cleanup()
	cleanSrc := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
	_, year, ok := recentYearDocSubquery(db, cleanSrc)
	return year, ok
}

// reportCleanReady — clean 단계 완료 판정. 빌드 잡 status는 "completed", artifact status는
// "ready"로 나뉘므로 둘 다 허용. status가 비어도 summary가 있으면 ready로 본다.
func reportCleanReady(version domain.DatasetVersion) bool {
	status := strings.ToLower(strings.TrimSpace(version.CleanStatus))
	if status == "ready" || status == "completed" {
		return true
	}
	return version.CleanSummary != nil
}

func reportArtifactRef(meta map[string]any, keys ...string) string {
	for _, key := range keys {
		if v := metadataString(meta, key, ""); strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// ── 블록/패널 빌드 ─────────────────────────────────────────────────────────

func (s *DatasetService) buildReportBlock(version domain.DatasetVersion, section registry.ReportTemplateSection, cache map[string]reportBuildRoot, labels reportLabels) map[string]any {
	layout := make([]any, 0, len(section.Layout))
	for _, row := range section.Layout {
		panels := make([]any, 0, len(row.Panels))
		for _, panel := range row.Panels {
			panels = append(panels, s.buildReportPanel(version, panel, cache, labels, section.DateFilter))
		}
		layout = append(layout, map[string]any{"panels": panels})
	}
	block := map[string]any{
		"block_id":   id.New(),
		"section_id": section.ID,
		"title":      section.Title,
		"layout":     layout,
	}
	if strings.TrimSpace(section.UnitBasis) != "" {
		block["unit_basis"] = section.UnitBasis
	}
	return block
}

func (s *DatasetService) buildReportPanel(version domain.DatasetVersion, panel registry.ReportTemplatePanel, cache map[string]reportBuildRoot, labels reportLabels, dateFilter string) map[string]any {
	out := map[string]any{"view": panel.View, "width": panel.Width}
	if strings.TrimSpace(panel.Title) != "" {
		out["title"] = panel.Title
	}

	if panel.View == "stat_grid" {
		out["data"] = s.statGridData(version, panel, cache, labels, dateFilter)
		return out
	}

	// source: metric 별칭(catalog) 우선, 없으면 직접 source. value_format도 함께 resolve.
	valueFormat := strings.TrimSpace(panel.ValueFormat)
	src := resolvePanelSource(panel, &valueFormat)
	if valueFormat != "" {
		out["value_format"] = valueFormat
	}
	if src == nil {
		out["data"] = map[string]any{}
		return out
	}
	root := s.reportBuildRoot(version, src.Build, dateFilter, cache)
	node := digPath(root.root, src.Path)
	switch panel.View {
	case "bar", "doughnut", "table":
		out["data"] = distributionData(node, src, labels)
	case "stacked_bar":
		out["data"] = stackedData(node, labels)
	case "rank":
		out["data"] = rankData(node, src, labels)
	case "period_timeline": // #31 분석 기간(연도별 대상기간·축제기간·기준/비교)
		out["data"] = periodTimelineData(node)
	case "tag_list": // #31 수집 채널/키워드
		out["data"] = tagListData(node)
	case "definition_list": // #31 유형 정의
		out["data"] = definitionListData(node)
	default:
		out["data"] = map[string]any{}
	}
	// source binding(갱신용). 프론트는 렌더에 안 씀.
	out["source"] = map[string]any{
		"kind":        "dataset_version_artifact",
		"version_id":  version.DatasetVersionID,
		"build_type":  src.Build,
		"source_path": src.Path,
	}
	return out
}

// ── transformer: distribution (bar/doughnut/table) ────────────────────────

func distributionData(node any, src *registry.ReportTemplateSource, labels reportLabels) map[string]any {
	counts, _ := asMap(node)
	type item struct {
		key   string
		count float64
	}
	rawItems := make([]item, 0, len(counts))
	total := 0.0
	for key, v := range counts {
		c, ok := toFloat(v)
		if !ok {
			continue
		}
		rawItems = append(rawItems, item{key: key, count: c})
		total += c
	}
	// 정렬: 고정 order 우선, 아니면 count 내림차순.
	if len(src.Order) > 0 {
		rank := map[string]int{}
		for i, k := range src.Order {
			rank[k] = i
		}
		sort.SliceStable(rawItems, func(i, j int) bool {
			ri, oki := rank[rawItems[i].key]
			rj, okj := rank[rawItems[j].key]
			if oki && okj {
				return ri < rj
			}
			if oki != okj {
				return oki
			}
			return rawItems[i].count > rawItems[j].count
		})
	} else {
		sort.SliceStable(rawItems, func(i, j int) bool { return rawItems[i].count > rawItems[j].count })
	}
	if src.Top > 0 && len(rawItems) > src.Top {
		rawItems = rawItems[:src.Top]
	}
	items := make([]any, 0, len(rawItems))
	for _, it := range rawItems {
		pct := 0.0
		if total > 0 {
			pct = round1(it.count / total * 100)
		}
		items = append(items, map[string]any{
			"key":     it.key,
			"label":   labels.label(src.Path, it.key),
			"count":   it.count,
			"percent": pct,
		})
	}
	return map[string]any{"total": total, "items": items}
}

// ── transformer: stacked_bar (aspect_sentiment) ───────────────────────────

func stackedData(node any, labels reportLabels) map[string]any {
	aspectMap, _ := asMap(node)
	type cat struct {
		key   string
		total float64
		sent  map[string]float64 // sentiment key → count
	}
	cats := make([]cat, 0, len(aspectMap))
	for key, v := range aspectMap {
		obj, ok := asMap(v)
		if !ok {
			continue
		}
		total, _ := toFloat(obj["total"])
		sent := map[string]float64{}
		if sm, ok := asMap(obj["sentiment"]); ok {
			for sk, sv := range sm {
				if so, ok := asMap(sv); ok {
					c, _ := toFloat(so["count"])
					sent[sk] = c
				}
			}
		}
		cats = append(cats, cat{key: key, total: total, sent: sent})
	}
	sort.SliceStable(cats, func(i, j int) bool { return cats[i].total > cats[j].total })

	categories := make([]any, 0, len(cats))
	for _, c := range cats {
		categories = append(categories, map[string]any{
			"key": c.key, "label": labels.label("summary.aspect", c.key), "total": c.total,
		})
	}
	series := make([]any, 0, 3)
	for _, sk := range []string{"positive", "neutral", "negative"} {
		counts := make([]any, 0, len(cats))
		percents := make([]any, 0, len(cats))
		for _, c := range cats {
			cnt := c.sent[sk]
			counts = append(counts, cnt)
			pct := 0.0
			if c.total > 0 {
				pct = round1(cnt / c.total * 100)
			}
			percents = append(percents, pct)
		}
		series = append(series, map[string]any{
			"key": sk, "label": labels.sentiment[sk], "counts": counts, "percents": percents,
		})
	}
	return map[string]any{"categories": categories, "series": series}
}

// ── transformer: rank (키워드 리스트 또는 aspect_sentiment order_by) ─────────

func rankData(node any, src *registry.ReportTemplateSource, labels reportLabels) map[string]any {
	// case 1: 리스트(키워드 top) — [{keyword,count} | {label,value}]
	if list, ok := asList(node); ok {
		items := make([]any, 0, len(list))
		n := len(list)
		if src.Top > 0 && src.Top < n {
			n = src.Top
		}
		for i := 0; i < n; i++ {
			row, ok := asMap(list[i])
			if !ok {
				continue
			}
			label := firstString(row, "keyword", "label", "key")
			value, _ := toFloat(firstAny(row, "count", "value", "weight"))
			items = append(items, map[string]any{"rank": i + 1, "label": label, "value": value})
		}
		return map[string]any{"items": items}
	}
	// case 2: aspect_sentiment map + order_by(positive|negative) → 그 감성 count로 순위.
	aspectMap, ok := asMap(node)
	if !ok {
		return map[string]any{"items": []any{}}
	}
	orderBy := strings.TrimSpace(src.OrderBy)
	if orderBy == "" {
		orderBy = "positive"
	}
	type row struct {
		key   string
		value float64
	}
	rows := make([]row, 0, len(aspectMap))
	for key, v := range aspectMap {
		obj, ok := asMap(v)
		if !ok {
			continue
		}
		value := 0.0
		if sm, ok := asMap(obj["sentiment"]); ok {
			if so, ok := asMap(sm[orderBy]); ok {
				value, _ = toFloat(so["count"])
			}
		}
		rows = append(rows, row{key: key, value: value})
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].value > rows[j].value })
	if src.Top > 0 && len(rows) > src.Top {
		rows = rows[:src.Top]
	}
	items := make([]any, 0, len(rows))
	for i, r := range rows {
		items = append(items, map[string]any{"rank": i + 1, "label": labels.label("summary.aspect", r.key), "value": r.value})
	}
	return map[string]any{"items": items}
}

// ── transformer: period_timeline (#31 분석 기간 — 연도별 대상기간·축제기간·기준/비교) ──
// analysisPeriodsView가 만든 [{year,role,role_label,target_start,target_end,target_days,
// festival_start,festival_end}]를 화면 타임라인 rows로 싣는다.
func periodTimelineData(node any) map[string]any {
	list, _ := asList(node)
	rows := make([]any, 0, len(list))
	for _, item := range list {
		m, ok := asMap(item)
		if !ok {
			continue
		}
		year, _ := anyToInt(m["year"])
		role, _ := m["role"].(string)
		roleLabel, _ := m["role_label"].(string)
		targetDays, _ := anyToInt(m["target_days"])
		targetStart, _ := m["target_start"].(string)
		targetEnd, _ := m["target_end"].(string)
		festivalStart, _ := m["festival_start"].(string)
		festivalEnd, _ := m["festival_end"].(string)
		rows = append(rows, map[string]any{
			"year":           year,
			"role":           role,
			"role_label":     roleLabel,
			"target_start":   targetStart,
			"target_end":     targetEnd,
			"target_days":    targetDays,
			"festival_start": festivalStart,
			"festival_end":   festivalEnd,
		})
	}
	return map[string]any{"rows": rows}
}

// ── transformer: tag_list (#31 수집 채널/키워드) ────────────────────────────
func tagListData(node any) map[string]any {
	list, _ := asList(node)
	items := make([]any, 0, len(list))
	for _, item := range list {
		s, ok := item.(string)
		if !ok {
			continue
		}
		if s = strings.TrimSpace(s); s != "" {
			items = append(items, s)
		}
	}
	return map[string]any{"items": items}
}

// ── transformer: definition_list (#31 유형 정의) ────────────────────────────
// type_definitions [{key,label,description}]를 {term,description} 목록으로.
func definitionListData(node any) map[string]any {
	list, _ := asList(node)
	items := make([]any, 0, len(list))
	for _, item := range list {
		m, ok := asMap(item)
		if !ok {
			continue
		}
		term, _ := m["label"].(string)
		if strings.TrimSpace(term) == "" {
			term, _ = m["key"].(string)
		}
		desc, _ := m["description"].(string)
		items = append(items, map[string]any{"term": strings.TrimSpace(term), "description": strings.TrimSpace(desc)})
	}
	return map[string]any{"items": items}
}

// ── transformer: stat_grid ────────────────────────────────────────────────

func (s *DatasetService) statGridData(version domain.DatasetVersion, panel registry.ReportTemplatePanel, cache map[string]reportBuildRoot, labels reportLabels, dateFilter string) map[string]any {
	items := make([]any, 0, len(panel.Items))
	for _, cfg := range panel.Items {
		// metric 별칭이면 catalog에서 source/format/unit/sub/label을 채우고, item에 명시된
		// 값이 있으면 그게 우선(override). metric 없이 source 직접 방식도 그대로 동작.
		source, subSource, format, unit, label, key := resolveStatItem(cfg)

		out := map[string]any{"key": key, "label": label, "format": format}
		if strings.TrimSpace(unit) != "" {
			out["unit"] = unit
		}
		var value any = cfg.Value
		if source != nil {
			root := s.reportBuildRoot(version, source.Build, dateFilter, cache)
			if resolved := digPath(root.root, source.Path); resolved != nil {
				value = normalizeStatValue(resolved)
			}
		}
		out["value"] = value
		if subSource != nil {
			root := s.reportBuildRoot(version, subSource.Build, dateFilter, cache)
			if resolved := digPath(root.root, subSource.Path); resolved != nil {
				out["sub"] = normalizeStatValue(resolved)
			}
		}
		items = append(items, out)
	}
	return map[string]any{"items": items}
}

// resolveStatItem — stat item의 metric 별칭을 catalog로 풀어 (source, subSource, format,
// unit, label, key)를 정한다. item에 명시된 값이 metric 기본값보다 우선.
func resolveStatItem(cfg registry.ReportTemplateStatItem) (source, subSource *registry.ReportTemplateSource, format, unit, label, key string) {
	source, subSource = cfg.Source, cfg.SubSource
	format, unit, label, key = cfg.Format, cfg.Unit, cfg.Label, cfg.Key
	if m, ok := registry.ReportMetricByID(cfg.Metric); ok {
		if source == nil {
			source = m.Source
		}
		if subSource == nil {
			subSource = m.SubSource
		}
		if format == "" {
			format = m.Format
		}
		if unit == "" {
			unit = m.Unit
		}
		if label == "" {
			label = m.Label
		}
		if key == "" {
			key = strings.TrimSpace(cfg.Metric)
		}
	}
	return source, subSource, format, unit, label, key
}

// resolvePanelSource — 차트 panel의 metric 별칭을 catalog source로 풀고, panel의
// order/order_by/top을 그 위에 덮어쓴다. value_format도 (panel 명시 없으면) metric 기본.
func resolvePanelSource(panel registry.ReportTemplatePanel, valueFormat *string) *registry.ReportTemplateSource {
	if strings.TrimSpace(panel.Metric) != "" {
		if m, ok := registry.ReportMetricByID(panel.Metric); ok && m.Source != nil {
			merged := *m.Source // copy
			if len(panel.Order) > 0 {
				merged.Order = panel.Order
			}
			if strings.TrimSpace(panel.OrderBy) != "" {
				merged.OrderBy = panel.OrderBy
			}
			if panel.Top > 0 {
				merged.Top = panel.Top
			}
			if *valueFormat == "" {
				*valueFormat = m.ValueFormat
			}
			return &merged
		}
	}
	return panel.Source
}

func normalizeStatValue(v any) any {
	if f, ok := toFloat(v); ok {
		return f
	}
	return v
}

// ── 라벨 (sentiment/genuineness 고정 + aspect taxonomy) ─────────────────────

type reportLabels struct {
	sentiment    map[string]string
	genuineness  map[string]string
	aspect       map[string]string
}

func (s *DatasetService) newReportLabels(version domain.DatasetVersion) reportLabels {
	labels := reportLabels{
		sentiment: map[string]string{
			"positive": "긍정", "neutral": "중립", "negative": "부정",
		},
		genuineness: map[string]string{
			"genuine_review": "진성 후기", "non_review": "비후기", "uncertain": "불확실",
		},
		aspect: map[string]string{},
	}
	taxonomyID := ""
	if m, ok := version.Metadata["clause_label_summary"].(map[string]any); ok {
		taxonomyID = strings.TrimSpace(metadataString(m, "taxonomy_id", ""))
	}
	if taxonomyID == "" {
		taxonomyID = "festival-gunsan"
	}
	labels.aspect = loadAspectLabels(taxonomyID)
	return labels
}

// label — source path로 라벨 종류를 추론해 key→한글 라벨. 미상이면 key 그대로.
func (l reportLabels) label(path, key string) string {
	p := strings.ToLower(path)
	switch {
	case strings.Contains(p, "sentiment") && !strings.Contains(p, "aspect_sentiment"):
		if v, ok := l.sentiment[key]; ok {
			return v
		}
	case strings.Contains(p, "genuineness"):
		if v, ok := l.genuineness[key]; ok {
			return v
		}
	case strings.Contains(p, "aspect"):
		if v, ok := l.aspect[key]; ok {
			return v
		}
	}
	if v, ok := l.aspect[key]; ok {
		return v
	}
	if v, ok := l.sentiment[key]; ok {
		return v
	}
	return key
}

func loadAspectLabels(taxonomyID string) map[string]string {
	out := map[string]string{}
	path := filepath.Join(registry.ConfigDir(), "taxonomies", taxonomyID+".json")
	content, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	var parsed struct {
		Aspects []struct {
			Key   string `json:"key"`
			Label string `json:"label"`
		} `json:"aspects"`
	}
	if err := json.Unmarshal(content, &parsed); err != nil {
		return out
	}
	for _, a := range parsed.Aspects {
		if a.Key != "" && a.Label != "" {
			out[a.Key] = a.Label
		}
	}
	return out
}

// ── 분석 개요(#31) 헬퍼 ───────────────────────────────────────────────────────

// festivalMeta — project.metadata.festival에서 축제명 + 정규화된 periods를 꺼낸다.
// 저장 시 normalizeFestivalMetadata로 검증되며, postgres JSONB 라운드트립으로 periods는
// []any(map[string]any)로 돌아오므로 그 형태를 흡수한다.
func festivalMeta(project domain.Project) (string, []map[string]any) {
	fm, ok := project.Metadata["festival"].(map[string]any)
	if !ok {
		return "", nil
	}
	name, _ := fm["name"].(string)
	rawPeriods, _ := fm["periods"].([]any)
	periods := make([]map[string]any, 0, len(rawPeriods))
	for _, rp := range rawPeriods {
		if pm, ok := rp.(map[string]any); ok {
			periods = append(periods, pm)
		}
	}
	return strings.TrimSpace(name), periods
}

// analysisPeriodsView — 저장된 축제 메타 periods를 화면 타임라인 rows로 만든다(2026-07-02
// 재설계). 연도별로 대상기간(target)·축제기간(festival)·역할(기준/비교)을 그대로 싣고,
// target_days(대상기간 총 일수, 양끝 포함)를 계산한다. 정렬: 기준 연도(base) 먼저, 그다음
// 연도 내림차순. 옛 before/during/after ±N일 파생은 폐기됐다.
func analysisPeriodsView(periods []map[string]any) []map[string]any {
	const dayFmt = "2006-01-02"
	type row struct {
		year  int
		base  bool
		entry map[string]any
	}
	rows := make([]row, 0, len(periods))
	for _, p := range periods {
		year, _ := anyToInt(p["year"])
		role, _ := p["role"].(string)
		if role == "" {
			role = festivalRoleCompare
		}
		targetStart, _ := p["target_start"].(string)
		targetEnd, _ := p["target_end"].(string)
		festivalStart, _ := p["festival_start"].(string)
		festivalEnd, _ := p["festival_end"].(string)
		roleLabel := "비교 연도"
		if role == festivalRoleBase {
			roleLabel = "기준 연도"
		}
		targetDays := 0
		if ts, err1 := time.Parse(dayFmt, strings.TrimSpace(targetStart)); err1 == nil {
			if te, err2 := time.Parse(dayFmt, strings.TrimSpace(targetEnd)); err2 == nil && !te.Before(ts) {
				targetDays = int(te.Sub(ts).Hours()/24) + 1 // 양끝 포함
			}
		}
		rows = append(rows, row{
			year: year,
			base: role == festivalRoleBase,
			entry: map[string]any{
				"year":           year,
				"role":           role,
				"role_label":     roleLabel,
				"target_start":   targetStart,
				"target_end":     targetEnd,
				"target_days":    targetDays,
				"festival_start": festivalStart,
				"festival_end":   festivalEnd,
			},
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].base != rows[j].base {
			return rows[i].base // 기준 연도 먼저
		}
		return rows[i].year > rows[j].year // 그다음 연도 내림차순
	})
	out := make([]map[string]any, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.entry)
	}
	return out
}

// datasetSubjectName — dataset.metadata.doc_genuineness.subject_name(운영자 지정 대상명).
func datasetSubjectName(dataset domain.Dataset) string {
	dg, ok := dataset.Metadata["doc_genuineness"].(map[string]any)
	if !ok {
		return ""
	}
	name, _ := dg["subject_name"].(string)
	return strings.TrimSpace(name)
}

// loadCollectionValues — clean source_json에서 후보 컬럼 중 값이 가장 많은 컬럼을 골라
// distinct 값 목록을 돌려준다(수집 채널/키워드 공용). splitPipe면 "A | B" 파이프 구분값을
// 분해한다. 채널/키워드 컬럼명은 데이터셋마다 달라(값 기반 자동선택, channel_breakdown과 동일).
func loadCollectionValues(cleanRef string, candidates []string, splitPipe bool) []string {
	if strings.TrimSpace(cleanRef) == "" {
		return nil
	}
	db, cleanup, err := openTempDuckDB()
	if err != nil {
		return nil
	}
	defer cleanup()
	cleanSrc := fmt.Sprintf("read_parquet('%s')", escapeDuckDBLiteral(cleanRef))
	valExpr := func(field string) string {
		return fmt.Sprintf(`json_extract_string(source_json, '$."%s"')`, field)
	}
	chosen, best := "", -1
	for _, f := range candidates {
		expr := valExpr(f)
		var cnt int
		q := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s IS NOT NULL AND %s <> ''", cleanSrc, expr, expr)
		if err := db.QueryRow(q).Scan(&cnt); err != nil {
			continue
		}
		if cnt > best {
			best, chosen = cnt, f
		}
	}
	if chosen == "" || best <= 0 {
		return nil
	}
	expr := valExpr(chosen)
	rows, err := db.Query(fmt.Sprintf("SELECT DISTINCT %s AS v FROM %s WHERE %s IS NOT NULL AND %s <> ''", expr, cleanSrc, expr, expr))
	if err != nil {
		return nil
	}
	defer rows.Close()
	set := map[string]bool{}
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil || !v.Valid {
			continue
		}
		parts := []string{v.String}
		if splitPipe {
			parts = strings.Split(v.String, "|")
		}
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				set[p] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// resolveReportTaxonomyID — clause_label_summary.taxonomy_id 우선 → dataset.metadata.
// taxonomy_id → 기본(festival-gunsan). loadAspectLabels와 동일 정책에 dataset fallback 추가.
func resolveReportTaxonomyID(version domain.DatasetVersion, dataset domain.Dataset) string {
	if m, ok := version.Metadata["clause_label_summary"].(map[string]any); ok {
		if id := strings.TrimSpace(metadataString(m, "taxonomy_id", "")); id != "" {
			return id
		}
	}
	if id := strings.TrimSpace(metadataString(dataset.Metadata, "taxonomy_id", "")); id != "" {
		return id
	}
	return "festival-gunsan"
}

// loadTypeDefinitions — taxonomy의 aspect 정의(유형 정의)를 {key,label,description} 목록으로.
func loadTypeDefinitions(taxonomyID string) []map[string]any {
	path := filepath.Join(registry.ConfigDir(), "taxonomies", taxonomyID+".json")
	content, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var parsed struct {
		Aspects []struct {
			Key         string `json:"key"`
			Label       string `json:"label"`
			Description string `json:"description"`
		} `json:"aspects"`
	}
	if err := json.Unmarshal(content, &parsed); err != nil {
		return nil
	}
	out := make([]map[string]any, 0, len(parsed.Aspects))
	for _, a := range parsed.Aspects {
		if strings.TrimSpace(a.Key) == "" {
			continue
		}
		out = append(out, map[string]any{"key": a.Key, "label": a.Label, "description": a.Description})
	}
	return out
}

var (
	collectionChannelCandidates = []string{"수집채널", "채널", "channel", "source", "platform", "매체", "미디어"}
	collectionKeywordCandidates = []string{"수집키워드", "검색키워드", "키워드", "keyword", "query", "검색어"}
)

// ── 공통 헬퍼 ──────────────────────────────────────────────────────────────

func digPath(root map[string]any, path string) any {
	if root == nil {
		return nil
	}
	parts := strings.Split(strings.TrimSpace(path), ".")
	var cur any = root
	for _, part := range parts {
		m, ok := asMap(cur)
		if !ok {
			return nil
		}
		cur, ok = m[part]
		if !ok {
			return nil
		}
	}
	return cur
}

// asMap — map[string]any 외에 map[string]int / map[string]float64 등 임의의
// string-keyed 맵을 map[string]any로 정규화. DuckDB 집계 로더(aggregateGroupedCounts
// 등)가 map[string]int를 반환하므로 digPath·transformer가 그대로 못 읽는 문제 대응.
func asMap(v any) (map[string]any, bool) {
	if v == nil {
		return nil, false
	}
	if m, ok := v.(map[string]any); ok {
		return m, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Map || rv.Type().Key().Kind() != reflect.String {
		return nil, false
	}
	out := make(map[string]any, rv.Len())
	for _, k := range rv.MapKeys() {
		out[k.String()] = rv.MapIndex(k).Interface()
	}
	return out, true
}

// asList — []any 외에 []map[string]any 등 임의 슬라이스를 []any로 정규화.
func asList(v any) ([]any, bool) {
	if v == nil {
		return nil, false
	}
	if l, ok := v.([]any); ok {
		return l, true
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
		return nil, false
	}
	out := make([]any, rv.Len())
	for i := 0; i < rv.Len(); i++ {
		out[i] = rv.Index(i).Interface()
	}
	return out, true
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		var f float64
		if _, err := json.Number(strings.TrimSpace(n)).Float64(); err == nil {
			f, _ = json.Number(strings.TrimSpace(n)).Float64()
			return f, true
		}
		return f, false
	}
	return 0, false
}

func round1(f float64) float64 {
	return math.Round(f*10) / 10
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return s
			}
		}
	}
	return ""
}

func firstAny(m map[string]any, keys ...string) any {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			return v
		}
	}
	return nil
}
