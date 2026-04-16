export type MessageRole = 'user' | 'assistant'

export interface ChatMessage {
  id: string
  role: MessageRole
  content: string
  timestamp: string
}

export interface SuggestedQuestion {
  id: string
  label: string
}

export interface ChatScenario {
  id: string
  name: string
  description: string
  category: string
}

export interface MockProject {
  id: string
  name: string
}

// ── 프로젝트 목록 ──────────────────────────────────────────
export const MOCK_PROJECTS: MockProject[] = [
  { id: 'p1', name: 'festival-manual' },
  { id: 'p2', name: 'semantic-search-smoke' },
  { id: 'p3', name: 'cluster-smoke issue' },
]

// ── 분석 시나리오 ──────────────────────────────────────────
export const MOCK_SCENARIOS: ChatScenario[] = [
  { id: 's1', name: '감성 분석',     description: '텍스트 긍/부정 분류', category: '텍스트' },
  { id: 's2', name: '트렌드 분석',   description: '시계열 패턴 탐지',    category: '시계열' },
  { id: 's3', name: '이상치 탐지',   description: '통계적 이상 식별',    category: '통계' },
  { id: 's4', name: '고객 세그먼트', description: 'RFM 기반 군집화',     category: '마케팅' },
  { id: 's5', name: '예측 모델링',   description: '수요 예측 모델',       category: 'ML' },
  { id: 's6', name: '상관관계 분석', description: '변수 인과 분석',      category: '통계' },
]

// ── 예상 질문 ──────────────────────────────────────────────
export const MOCK_SUGGESTED_QUESTIONS: SuggestedQuestion[] = [
  { id: 'q1', label: '매출 트렌드를 분석해줘' },
  { id: 'q2', label: '이상치가 있는지 확인해줘' },
  { id: 'q3', label: '고객 세그먼트 현황은?' },
  { id: 'q4', label: '지난 달 대비 증감률은?' },
]

// ── 초기 메시지 ────────────────────────────────────────────
export const MOCK_INITIAL_MESSAGES: ChatMessage[] = [
  {
    id: 'msg-0',
    role: 'assistant',
    content:
      '안녕하세요! 데이터 분석을 시작하겠습니다.\n오른쪽 패널에서 분석 시나리오를 선택하거나, 아래 예상 질문을 클릭하거나, 직접 질문을 입력해 주세요.',
    timestamp: '09:00',
  },
]

// ── 시나리오 선택 시 assistant 응답 템플릿 ─────────────────
export function makeScenarioReply(scenarioName: string, projectName: string): ChatMessage {
  return {
    id: crypto.randomUUID(),
    role: 'assistant',
    content: `**[${scenarioName}]** 시나리오를 시작합니다.\n\n연결된 프로젝트 **${projectName}** 의 데이터셋을 불러오는 중입니다...\n준비가 완료되면 분석을 시작하겠습니다.`,
    timestamp: new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
  }
}

// ── 일반 질문에 대한 mock 응답 ─────────────────────────────
export function makeMockReply(userMessage: string): ChatMessage {
  const replies: Record<string, string> = {
    '매출 트렌드를 분석해줘':
      '연결된 데이터셋을 기반으로 매출 트렌드를 분석했습니다.\n\n- **11월**: 전월 대비 +12.3% 상승\n- **12월**: 시즌 효과로 +28.1% 급등\n- **1월**: 정상화 구간 -8.4%\n\n전반적으로 **우상향 추세**를 보이고 있으며, 12월 피크 이후 조정이 나타나고 있습니다.',
    '이상치가 있는지 확인해줘':
      'IQR 기법으로 이상치를 탐지한 결과, **11월 3주차** 데이터에서 통계적 이상치가 발견되었습니다.\n\n- 탐지값: 평균 대비 **+2.8σ** 이탈\n- 가능한 원인: 프로모션 이벤트 또는 데이터 입력 오류\n\n해당 기간 원본 데이터를 검토해보시겠어요?',
    '고객 세그먼트 현황은?':
      'RFM 분석 기반 현재 고객 세그먼트 현황입니다.\n\n| 세그먼트 | 비율 |\n|---|---|\n| VIP | 8.2% |\n| 충성고객 | 21.5% |\n| 잠재고객 | 34.1% |\n| 이탈위험 | 36.2% |\n\n이탈위험 고객 비율이 높아 **재활성화 캠페인**을 검토해 보시길 권장합니다.',
  }

  const content =
    replies[userMessage] ??
    `"${userMessage}"에 대한 분석을 진행하겠습니다. 연결된 데이터셋을 참조하여 결과를 생성 중입니다...`

  return {
    id: crypto.randomUUID(),
    role: 'assistant',
    content,
    timestamp: new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
  }
}