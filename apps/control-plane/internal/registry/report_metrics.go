package registry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// metric catalog — config/report_metrics.json. 운영자 템플릿이 내부 build/path 대신
// metric 이름만 쓰게 하는 별칭 사전. report engine이 metric을 source로 resolve한다.

type reportMetricsFile struct {
	Metrics map[string]ReportMetric `json:"metrics"`
}

var (
	reportMetricsLoadOnce sync.Once
	cachedReportMetrics   map[string]ReportMetric
	reportMetricsLoadErr  error
)

func reportMetrics() (map[string]ReportMetric, error) {
	reportMetricsLoadOnce.Do(func() {
		cachedReportMetrics, reportMetricsLoadErr = loadReportMetrics()
	})
	return cachedReportMetrics, reportMetricsLoadErr
}

// ReportMetricByID — metric 이름으로 catalog lookup. 없으면 (zero, false).
func ReportMetricByID(name string) (ReportMetric, bool) {
	metrics, err := reportMetrics()
	if err != nil {
		return ReportMetric{}, false
	}
	m, ok := metrics[strings.TrimSpace(name)]
	return m, ok
}

// ListReportMetrics — 사용 가능한 metric 사전(운영자 문서/검증용).
func ListReportMetrics() (map[string]ReportMetric, error) {
	return reportMetrics()
}

func loadReportMetrics() (map[string]ReportMetric, error) {
	path := resolveReportMetricsPath()
	content, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]ReportMetric{}, nil // 없으면 빈 catalog(에러 아님 — source 직접 방식만 동작)
		}
		return nil, fmt.Errorf("read report metrics %s: %w", path, err)
	}
	var parsed reportMetricsFile
	if err := json.Unmarshal(content, &parsed); err != nil {
		return nil, fmt.Errorf("decode report metrics %s: %w", path, err)
	}
	out := make(map[string]ReportMetric, len(parsed.Metrics))
	for name, m := range parsed.Metrics {
		name = strings.TrimSpace(name)
		if name == "" || m.Source == nil {
			return nil, fmt.Errorf("report metric %q must have a non-empty name and source (%s)", name, path)
		}
		out[name] = m
	}
	return out, nil
}

func resolveReportMetricsPath() string {
	override := strings.TrimSpace(os.Getenv("REPORT_METRICS_PATH"))
	root := detectWorkspaceRoot()
	if override == "" {
		return filepath.Join(root, "config", "report_metrics.json")
	}
	if filepath.IsAbs(override) {
		return override
	}
	return filepath.Join(root, override)
}
