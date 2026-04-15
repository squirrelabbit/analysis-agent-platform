import { BrowserRouter, Navigate, Route, Routes } from 'react-router-dom'
import AppLayout from './layout/AppLayout'
import { ChatPage } from './pages/ChatPage'
import ProjectPage from './pages/ProjectPage'

const workflowSteps = [
  {
    title: 'Dataset',
    description: '업로드한 데이터와 profile, build job 상태를 한 화면에서 확인합니다.',
    status: '먼저 구현',
  },
  {
    title: 'Scenario',
    description: 'strict 시나리오 등록, import, one-shot execute를 연결합니다.',
    status: '바로 연결 가능',
  },
  {
    title: 'Execution',
    description: 'result_v1, warnings, waiting, step 결과를 제품 화면으로 노출합니다.',
    status: '핵심 화면',
  },
  {
    title: 'Report Draft',
    description: '선택한 execution snapshot으로 보고서 초안을 재사용합니다.',
    status: '후속 단계',
  },
]

const apiSurface = [
  'GET /projects/{project_id}/datasets/{dataset_id}/versions/{version_id}/build_jobs',
  'POST /projects/{project_id}/scenarios/{scenario_id}/execute',
  'GET /projects/{project_id}/executions',
  'GET /projects/{project_id}/executions/{execution_id}/result',
]

const todayFocus = [
  'Vite React 기반 운영 콘솔 스캐폴드',
  'control-plane API 연결을 위한 dev proxy 준비',
  'Dataset / Scenario / Execution 3개 흐름 우선 노출',
]

function App() {
  const apiBaseUrl =
    import.meta.env.VITE_API_BASE_URL?.trim() || 'http://127.0.0.1:18080'

  return (
    <BrowserRouter>
      <AppLayout>
        <Routes>
          <Route path="/"  element={<Navigate to="/chat" replace />}/>
          <Route path="/chat" element={<ChatPage />} />
          <Route path="/project" element={<ProjectPage />} />
        </Routes>
      </AppLayout>
    </BrowserRouter>
  )
}

export default App
