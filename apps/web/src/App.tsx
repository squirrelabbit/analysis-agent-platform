import './App.css'

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
    <main className="shell">
      <section className="hero">
        <div className="hero-copy">
          <p className="eyebrow">Analysis Support Platform</p>
          <h1>운영 콘솔 프론트엔드 시작점</h1>
          <p className="lede">
            현재 백엔드는 dataset build, strict scenario, execution result
            snapshot까지 준비돼 있습니다. 이 앱은 그 흐름을 Dataset,
            Scenario, Execution 화면으로 묶는 첫 번째 진입점입니다.
          </p>
          <div className="hero-actions">
            <a className="primary" href={apiBaseUrl} target="_blank" rel="noreferrer">
              Control Plane 열기
            </a>
            <a
              className="secondary"
              href={`${apiBaseUrl}/swagger`}
              target="_blank"
              rel="noreferrer"
            >
              Swagger 보기
            </a>
          </div>
        </div>

        <aside className="hero-panel">
          <p className="panel-label">기본 연결</p>
          <code>{apiBaseUrl}</code>
          <ul>
            {todayFocus.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        </aside>
      </section>

      <section className="grid-section">
        <header className="section-header">
          <p className="eyebrow">Build Plan</p>
          <h2>추천 구현 순서</h2>
        </header>
        <div className="card-grid">
          {workflowSteps.map((step, index) => (
            <article key={step.title} className="card">
              <p className="card-index">0{index + 1}</p>
              <h3>{step.title}</h3>
              <p>{step.description}</p>
              <span className="status-chip">{step.status}</span>
            </article>
          ))}
        </div>
      </section>

      <section className="split-section">
        <article className="detail-card">
          <p className="eyebrow">API Surface</p>
          <h2>먼저 붙일 엔드포인트</h2>
          <ul className="endpoint-list">
            {apiSurface.map((endpoint) => (
              <li key={endpoint}>
                <code>{endpoint}</code>
              </li>
            ))}
          </ul>
        </article>

        <article className="detail-card accent">
          <p className="eyebrow">Next UI</p>
          <h2>첫 화면 권장 구성</h2>
          <ol className="ordered-list">
            <li>Dataset version 목록과 build job 상태</li>
            <li>Scenario 목록과 one-shot execute 버튼</li>
            <li>Execution result_v1와 warnings 패널</li>
          </ol>
        </article>
      </section>
    </main>
  )
}

export default App
