package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// ReportTemplate — 보고서 "기본 템플릿" 정의. config/report_templates/*.json에서 로드.
// 섹션 구성·배치(layout)·표현(view/width/value_format)·출처(source)를 코드가 아닌 config로
// 둬서 운영자가 파일 수정만으로 섹션 추가/삭제/순서변경/뷰변경을 한다.
// (계약: docs/api/report_basic_template.sample.md)
type ReportTemplate struct {
	TemplateID  string                  `json:"template_id"`
	Name        string                  `json:"name"`
	DataType    string                  `json:"data_type"`
	ReportTitle string                  `json:"report_title"`
	Sections    []ReportTemplateSection `json:"sections"`
}

type ReportTemplateSection struct {
	ID            string              `json:"id"`
	Title         string              `json:"title"`
	RequiredBuild string              `json:"required_build"`
	UnitBasis     string              `json:"unit_basis,omitempty"`
	Phase         string              `json:"phase,omitempty"`
	// DateFilter — "recent_year"면 이 섹션 분포를 최신년도 데이터로만 집계한다(개요 섹션은
	// 미설정=전체). silverone 2026-06-25. clause_label/clause_keywords/channel_breakdown에만 적용.
	DateFilter string              `json:"date_filter,omitempty"`
	Layout     []ReportTemplateRow `json:"layout"`
}

type ReportTemplateRow struct {
	Panels []ReportTemplatePanel `json:"panels"`
}

type ReportTemplatePanel struct {
	View        string                   `json:"view"`
	Width       string                   `json:"width"`
	Metric      string                   `json:"metric,omitempty"` // metric catalog 별칭(source 대신)
	ValueFormat string                   `json:"value_format,omitempty"`
	Title       string                   `json:"title,omitempty"`
	// metric을 쓸 때 정렬·상위 N을 panel에서 덮어쓴다(metric의 source 위에 적용).
	OrderBy string   `json:"order_by,omitempty"`
	Order   []string `json:"order,omitempty"`
	Top     int      `json:"top,omitempty"`
	Items   []ReportTemplateStatItem `json:"items,omitempty"` // stat_grid 전용
	Source  *ReportTemplateSource    `json:"source,omitempty"`
}

// ReportTemplateSource — 패널 데이터를 어느 build summary의 어느 path에서, 어떻게 잘라올지.
type ReportTemplateSource struct {
	Build   string   `json:"build"`             // clean | doc_genuineness | clause_label | clause_keywords | channel_breakdown | version
	Path    string   `json:"path"`              // 예: summary.sentiment
	OrderBy string   `json:"order_by,omitempty"` // count | positive | negative ...
	Order   []string `json:"order,omitempty"`    // 고정 순서(키 나열)
	Top     int      `json:"top,omitempty"`
}

// ReportTemplateStatItem — stat_grid 항목. metric(별칭) / value(정적) / source(직접) 중 하나.
type ReportTemplateStatItem struct {
	Key       string                `json:"key,omitempty"`
	Label     string                `json:"label"`
	Metric    string                `json:"metric,omitempty"` // metric catalog 별칭(source/format/unit 자동)
	Format    string                `json:"format,omitempty"`
	Unit      string                `json:"unit,omitempty"`
	Value     any                   `json:"value,omitempty"`
	Source    *ReportTemplateSource `json:"source,omitempty"`
	SubSource *ReportTemplateSource `json:"sub_source,omitempty"`
}

// ReportMetric — metric catalog 항목(config/report_metrics.json). 운영자가 내부
// build/path를 몰라도 metric 이름만으로 stat/차트를 구성하게 한다.
type ReportMetric struct {
	Kind        string                `json:"kind"` // stat | distribution | stacked | rank
	Source      *ReportTemplateSource `json:"source"`
	SubSource   *ReportTemplateSource `json:"sub_source,omitempty"`
	Format      string                `json:"format,omitempty"`       // stat용 값 포맷
	Unit        string                `json:"unit,omitempty"`         // stat용 단위
	ValueFormat string                `json:"value_format,omitempty"` // 차트용 값 포맷
	Label       string                `json:"label,omitempty"`        // 기본 라벨(item이 안 주면)
}

var (
	reportTemplateLoadOnce sync.Once
	cachedReportTemplates  map[string]ReportTemplate
	reportTemplateLoadErr  error
)

func reportTemplates() (map[string]ReportTemplate, error) {
	reportTemplateLoadOnce.Do(func() {
		cachedReportTemplates, reportTemplateLoadErr = loadReportTemplates()
	})
	return cachedReportTemplates, reportTemplateLoadErr
}

// ReportTemplateByID — template_id로 lookup. 없으면 (zero, false).
func ReportTemplateByID(id string) (ReportTemplate, bool) {
	templates, err := reportTemplates()
	if err != nil {
		return ReportTemplate{}, false
	}
	template, ok := templates[strings.TrimSpace(id)]
	return template, ok
}

// ListReportTemplates — 사용 가능한 템플릿 목록(template_id 정렬).
func ListReportTemplates() ([]ReportTemplate, error) {
	templates, err := reportTemplates()
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(templates))
	for id := range templates {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]ReportTemplate, 0, len(ids))
	for _, id := range ids {
		out = append(out, templates[id])
	}
	return out, nil
}

func loadReportTemplates() (map[string]ReportTemplate, error) {
	dir := resolveReportTemplatesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]ReportTemplate{}, nil // 디렉토리 없으면 빈 목록(에러 아님)
		}
		return nil, fmt.Errorf("read report templates dir %s: %w", dir, err)
	}

	out := make(map[string]ReportTemplate)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
			continue
		}
		path := filepath.Join(dir, entry.Name())
		content, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read report template %s: %w", path, err)
		}
		var template ReportTemplate
		if err := json.Unmarshal(content, &template); err != nil {
			return nil, fmt.Errorf("decode report template %s: %w", path, err)
		}
		template.TemplateID = strings.TrimSpace(template.TemplateID)
		if template.TemplateID == "" {
			return nil, fmt.Errorf("report template %s is missing template_id", path)
		}
		if _, exists := out[template.TemplateID]; exists {
			return nil, fmt.Errorf("duplicate report template_id %s (%s)", template.TemplateID, path)
		}
		out[template.TemplateID] = template
	}
	return out, nil
}

func resolveReportTemplatesDir() string {
	override := strings.TrimSpace(os.Getenv("REPORT_TEMPLATES_PATH"))
	root := detectWorkspaceRoot()
	if override == "" {
		return filepath.Join(root, "config", "report_templates")
	}
	if filepath.IsAbs(override) {
		return override
	}
	return filepath.Join(root, override)
}
