package planner

import "analysis-support-platform/control-plane/internal/domain"

func testAnalysisInput() domain.AnalysisSubmitRequest {
	dataType := "unstructured"
	datasetName := "issues.csv"
	datasetVersionID := "dataset-version-1"
	return domain.AnalysisSubmitRequest{
		DatasetName:      &datasetName,
		DatasetVersionID: &datasetVersionID,
		DataType:         &dataType,
		Goal:             "고객 이슈를 요약해줘",
		Constraints:      []string{"최근 데이터 기준"},
		Context: map[string]any{
			"channel": "voc",
		},
	}
}
