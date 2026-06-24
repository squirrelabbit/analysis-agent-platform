package service

import (
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
// 채워지고, 아니면 missing_sections에 기록한다. summary는 빌드 때 계산된 걸 reshape할 뿐
// (추가 집계 없음). 생성된 블록엔 데이터 스냅샷 + source binding을 함께 저장(나중에 "갱신" 용).
// 계약: docs/api/report_basic_template.sample.md
func (s *DatasetService) CreateReportFromTemplate(projectID string, req domain.ReportFromTemplateRequest) (domain.ReportFromTemplateResponse, error) {
	if err := s.requireProject(projectID); err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	template, ok := registry.ReportTemplateByID(strings.TrimSpace(req.TemplateID))
	if !ok {
		return domain.ReportFromTemplateResponse{}, ErrInvalidArgument{Message: "unknown template_id"}
	}
	versionID := strings.TrimSpace(req.DatasetVersionID)
	if versionID == "" {
		return domain.ReportFromTemplateResponse{}, ErrInvalidArgument{Message: "dataset_version_id is required"}
	}

	rawVersion, err := s.store.GetDatasetVersion(projectID, versionID)
	if err != nil {
		if err == store.ErrNotFound {
			return domain.ReportFromTemplateResponse{}, ErrNotFound{Resource: "dataset version"}
		}
		return domain.ReportFromTemplateResponse{}, err
	}
	version, err := s.GetDatasetVersion(projectID, rawVersion.DatasetID, versionID)
	if err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	if !reportCleanReady(version) {
		return domain.ReportFromTemplateResponse{}, ErrInvalidArgument{Message: "clean_not_ready"}
	}

	labels := s.newReportLabels(version)
	roots := map[string]reportBuildRoot{} // build → (root, ready) 캐시

	blocks := make([]map[string]any, 0, len(template.Sections))
	included := make([]string, 0, len(template.Sections))
	missing := make([]domain.ReportMissingSection, 0)

	for _, section := range template.Sections {
		root := s.reportBuildRoot(version, section.RequiredBuild, roots)
		if !root.ready {
			missing = append(missing, domain.ReportMissingSection{
				SectionID: section.ID,
				Reason:    strings.TrimSpace(section.RequiredBuild) + "_not_ready",
			})
			continue
		}
		blocks = append(blocks, s.buildReportBlock(version, section, roots, labels))
		included = append(included, section.ID)
	}

	blocksJSON, err := json.Marshal(blocks)
	if err != nil {
		return domain.ReportFromTemplateResponse{}, err
	}
	now := time.Now().UTC()
	title := strings.TrimSpace(template.ReportTitle)
	if title == "" {
		title = "데이터 기초 분석 보고서"
	}
	report := domain.Report{
		ReportID:         id.New(),
		ProjectID:        projectID,
		Title:            title,
		DatasetVersionID: versionID,
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
		IncludedSections: included,
		MissingSections:  missing,
	}, nil
}

// ── build summary 루트 (캐시) ──────────────────────────────────────────────

type reportBuildRoot struct {
	root  map[string]any
	ready bool
}

func (s *DatasetService) reportBuildRoot(version domain.DatasetVersion, build string, cache map[string]reportBuildRoot) reportBuildRoot {
	build = strings.TrimSpace(build)
	if cached, ok := cache[build]; ok {
		return cached
	}
	result := s.loadReportBuildRoot(version, build)
	cache[build] = result
	return result
}

func (s *DatasetService) loadReportBuildRoot(version domain.DatasetVersion, build string) reportBuildRoot {
	switch build {
	case "version":
		datasetName := ""
		if ds, err := s.GetDataset(version.ProjectID, version.DatasetID); err == nil {
			datasetName = ds.Name
		}
		return reportBuildRoot{root: map[string]any{
			"dataset_name":  datasetName,
			"version_label": metadataString(version.Metadata, "version_label", ""),
			"data_period":   metadataString(version.Metadata, "data_period", ""),
			"lloa_model":    metadataString(version.Metadata, "clause_label_model", metadataString(version.Metadata, "doc_genuineness_model", "")),
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
		// ADR-028 verify 모드는 schema가 달라 전용 로더(final_label 기준 집계). view
		// 핸들러(dataset_artifact_views.go)와 동일 분기.
		var summary map[string]any
		var err error
		if metadataString(version.Metadata, "clause_label_mode", "") == "verify" {
			summary, _, _, _, err = loadClauseLabelVerifyArtifact(ref, 1, 0, "", "", false, false)
		} else {
			summary, _, _, _, err = loadClauseLabelArtifact(ref, 1, 0, "", "")
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
		summary, _, _, err := loadClauseKeywordsArtifact(ref, 1, 0, "", "", "", "")
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
		summary, err := loadChannelGenuineBreakdown(cleanRef, genRef, verify)
		if err != nil || summary == nil {
			return reportBuildRoot{}
		}
		return reportBuildRoot{root: map[string]any{"summary": summary}, ready: true}
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
func loadChannelGenuineBreakdown(cleanRef, genuinenessRef string, verifyMode bool) (map[string]any, error) {
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
			(SELECT doc_id, %s AS channel FROM %s) c
			JOIN (SELECT doc_id FROM %s WHERE %s = 'genuine_review') g ON c.doc_id = g.doc_id
		WHERE c.channel IS NOT NULL AND c.channel <> ''
		GROUP BY c.channel`,
		channelExpr(chosen), cleanSrc, genSrc, labelCol)
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
			panels = append(panels, s.buildReportPanel(version, panel, cache, labels))
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

func (s *DatasetService) buildReportPanel(version domain.DatasetVersion, panel registry.ReportTemplatePanel, cache map[string]reportBuildRoot, labels reportLabels) map[string]any {
	out := map[string]any{"view": panel.View, "width": panel.Width}
	if strings.TrimSpace(panel.ValueFormat) != "" {
		out["value_format"] = panel.ValueFormat
	}
	if strings.TrimSpace(panel.Title) != "" {
		out["title"] = panel.Title
	}

	if panel.View == "stat_grid" {
		out["data"] = s.statGridData(version, panel, cache, labels)
		return out
	}

	src := panel.Source
	if src == nil {
		out["data"] = map[string]any{}
		return out
	}
	root := s.reportBuildRoot(version, src.Build, cache)
	node := digPath(root.root, src.Path)
	switch panel.View {
	case "bar", "doughnut", "table":
		out["data"] = distributionData(node, src, labels)
	case "stacked_bar":
		out["data"] = stackedData(node, labels)
	case "rank":
		out["data"] = rankData(node, src, labels)
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

// ── transformer: stat_grid ────────────────────────────────────────────────

func (s *DatasetService) statGridData(version domain.DatasetVersion, panel registry.ReportTemplatePanel, cache map[string]reportBuildRoot, labels reportLabels) map[string]any {
	items := make([]any, 0, len(panel.Items))
	for _, cfg := range panel.Items {
		out := map[string]any{"key": cfg.Key, "label": cfg.Label, "format": cfg.Format}
		if strings.TrimSpace(cfg.Unit) != "" {
			out["unit"] = cfg.Unit
		}
		var value any = cfg.Value
		if cfg.Source != nil {
			root := s.reportBuildRoot(version, cfg.Source.Build, cache)
			if resolved := digPath(root.root, cfg.Source.Path); resolved != nil {
				value = normalizeStatValue(resolved)
			}
		}
		out["value"] = value
		if cfg.SubSource != nil {
			root := s.reportBuildRoot(version, cfg.SubSource.Build, cache)
			if resolved := digPath(root.root, cfg.SubSource.Path); resolved != nil {
				out["sub"] = normalizeStatValue(resolved)
			}
		}
		items = append(items, out)
	}
	return map[string]any{"items": items}
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
