export type Scenario = {
  project_id: string
  scenario_id: string
  planning_mode: string,
  user_query: string
  query_type: string
  interpretation: string
  analysis_scope: string
  // steps?: Step[],
  steps?: string,
  created_at: string
}

export const MOCK_SCENARIOS: Scenario[] = [
	{
		"project_id" : "8b7c49a4-37d3-466d-9c84-f81b69fea61b",
		"scenario_id" : "S1",
		"user_query" : "이번 벚꽃 축제 반응 어때?",
		"query_type" : "여론 요약",
		"interpretation" : "전체 여론 및 분위기 파악",
		"analysis_scope" : "축제 기간",
		"steps" : "[{\"step\": 1, \"function_name\": \"가비지 필터링\", \"result_description\": \"분석 대상 정제\", \"runtime_skill_name\": \"garbage_filter\"}, {\"step\": 2, \"parameters\": {\"top_n\": 10}, \"function_name\": \"빈도 기반 키워드 추출\", \"parameter_text\": \"top_n=10\", \"result_description\": \"주요 키워드\"}]",
		"created_at" : "2026-04-02T05:25:19.158Z",
		"planning_mode" : "strict"
	},
	{
		"project_id" : "e7ad8d32-0b7d-4b6a-a0b5-e41b0d06b45a",
		"scenario_id" : "S1",
		"user_query" : "이번 벚꽃 축제 반응 어때?",
		"query_type" : "여론 요약",
		"interpretation" : "전체 여론 및 분위기 파악",
		"analysis_scope" : "축제 기간",
		"steps" : "[{\"step\": 1, \"function_name\": \"가비지 필터링\", \"result_description\": \"분석 대상 정제\"}, {\"step\": 2, \"function_name\": \"빈도 기반 키워드 추출\", \"parameter_text\": \"top_n=10\", \"result_description\": \"주요 키워드\"}, {\"step\": 3, \"function_name\": \"전체 담론 요약\", \"result_description\": \"종합 의견 요약\", \"runtime_skill_name\": \"issue_evidence_summary\"}]",
		"created_at" : "2026-04-02T05:39:51.874Z",
		"planning_mode" : "strict"
	},
	{
		"project_id" : "35ce3621-2c5e-4ef4-be25-0b0efb8f4053",
		"scenario_id" : "S1",
		"user_query" : "이번 벚꽃 축제 반응 어때?",
		"query_type" : "여론 요약",
		"interpretation" : "전체 여론 및 분위기 파악",
		"analysis_scope" : "축제 기간",
		"steps" : "[{\"step\": 1, \"function_name\": \"가비지 필터링\", \"result_description\": \"분석 대상 정제\"}]",
		"created_at" : "2026-04-02T05:51:20.592Z",
		"planning_mode" : "strict"
	},
	{
		"project_id" : "69376a2f-f0e8-4528-985a-5b5d9a082249",
		"scenario_id" : "S1",
		"user_query" : "이번 벚꽃 축제 반응 어때?",
		"query_type" : "여론 요약",
		"interpretation" : "전체 여론 및 분위기 파악",
		"analysis_scope" : "축제 기간",
		"steps" : "[{\"step\": 1, \"function_name\": \"가비지 필터링\", \"result_description\": \"분석 대상 정제\", \"runtime_skill_name\": \"garbage_filter\"}, {\"step\": 2, \"parameters\": {\"top_n\": 10}, \"function_name\": \"빈도 기반 키워드 추출\", \"parameter_text\": \"top_n=10\", \"result_description\": \"주요 키워드\", \"runtime_skill_name\": \"keyword_frequency\"}]",
		"created_at" : "2026-04-02T06:01:37.701Z",
		"planning_mode" : "strict"
	},
	// {
	// 	"project_id" : "69376a2f-f0e8-4528-985a-5b5d9a082249",
	// 	"scenario_id" : "S2",
	// 	"user_query" : "이번 축제 문제 뭐였어?",
	// 	"query_type" : "이슈 분석",
	// 	"interpretation" : "부정 의견 중심 문제 파악",
	// 	"analysis_scope" : "축제 기간",
	// 	"steps" : "[{\"step\": 1, \"function_name\": \"문서 단위 감성 분류\", \"result_description\": \"감성 분류\", \"runtime_skill_name\": \"issue_sentiment_summary\"}]",
	// 	"created_at" : "2026-04-02T06:01:37.704Z",
	// 	"planning_mode" : "strict"
	// },
	// {
	// 	"project_id" : "a7fdb72a-adfc-47d8-9048-a7b91d6384cf",
	// 	"scenario_id" : "S1",
	// 	"user_query" : "이번 벚꽃 축제 반응 어때?",
	// 	"query_type" : "여론 요약",
	// 	"interpretation" : "전체 여론 및 분위기 파악",
	// 	"analysis_scope" : "축제 기간",
	// 	"steps" : "[{\"step\": 1, \"function_name\": \"가비지 필터링\", \"result_description\": \"분석 대상 정제\", \"runtime_skill_name\": \"garbage_filter\"}, {\"step\": 2, \"function_name\": \"전체 담론 요약\", \"result_description\": \"종합 의견 요약\", \"runtime_skill_name\": \"issue_evidence_summary\"}]",
	// 	"created_at" : "2026-04-02T06:40:41.214Z",
	// 	"planning_mode" : "strict"
	// },
	{
		"project_id" : "6e356e77-b8fc-4fb9-97a7-537d329445c8",
		"scenario_id" : "S1",
		"user_query" : "이번 벚꽃 축제 반응 어때?",
		"query_type" : "여론 요약",
		"interpretation" : "전체 여론 및 분위기 파악",
		"analysis_scope" : "축제 기간",
		"steps" : "[{\"step\": 1, \"function_name\": \"가비지 필터링\", \"result_description\": \"분석 대상 정제\", \"runtime_skill_name\": \"garbage_filter\"}, {\"step\": 2, \"function_name\": \"감성 비율 집계\", \"result_description\": \"긍\/부정 비율\", \"runtime_skill_name\": \"issue_sentiment_summary\"}, {\"step\": 3, \"parameters\": {\"top_n\": 10}, \"function_name\": \"빈도 기반 키워드 추출\", \"parameter_text\": \"top_n=10\", \"result_description\": \"주요 키워드\", \"runtime_skill_name\": \"keyword_frequency\"}, {\"step\": 4, \"function_name\": \"전체 담론 요약\", \"result_description\": \"종합 의견 요약\", \"runtime_skill_name\": \"issue_evidence_summary\"}]",
		"created_at" : "2026-04-02T08:48:47.064Z",
		"planning_mode" : "strict"
	},]