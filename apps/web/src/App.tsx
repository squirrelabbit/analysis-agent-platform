import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ChatPage } from "./features/chats/pages/ChatPage";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProjectLayout from "./layout/ProjectLayout";
import ProjectPage from "./pages/ProjectPage";
// import DatasetDetail from "./features/datasets/pages/DatasetDetail"; // 리디자인으로 교체

// import DatasetContainer from "./features/datasets/pages/DatasetContainer"; // 리디자인으로 교체
import DatasetListRedesign from "./features/datasets/redesign/DatasetListRedesign";
import VersionDetailPage from "./features/versions/pages/VersionDetailPage";
import DatasetVersionListRedesign from "./features/versions/redesign/DatasetVersionListRedesign";
import DocGenuinenessComparePage from "./features/versions/pages/DocGenuinenessComparePage";
import LoginPage from "./features/auth/pages/LoginPage";
import ReportListPage from "./features/reports/pages/ReportListPage";
import ReportEditorPage from "./features/reports/pages/ReportEditorPage";
import { AnalyticsPage } from "./features/analytics/pages/AnalyticsPage";

const queryClient = new QueryClient();

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppLayout>
          <Routes>
            <Route path="/login" element={<LoginPage />} />
            <Route path="/" element={<Navigate to="/projects" replace />} />
            <Route path="/projects" element={<ProjectPage />} />
            <Route path="/projects/:projectId" element={<ProjectLayout />}>
              <Route index element={<Navigate to="datasets" replace />} />
              {/* 기존 데이터셋 목록 화면 (리디자인으로 교체, 보존) */}
              {/* <Route path="datasets" element={<DatasetContainer />} /> */}
              <Route path="datasets" element={<DatasetListRedesign />} />
              <Route path="datasets/:datasetId">
                <Route path="versions/:versionId" element={<VersionDetailPage /> }/> 
                <Route index element={<Navigate to="versions" replace />} />
                {/* 기존 버전 목록 화면 (리디자인으로 교체, 보존) */}
                {/* <Route path="versions" element={<DatasetDetail />} /> */}
                <Route path="versions" element={<DatasetVersionListRedesign />} />
                <Route path="doc-genuineness-compare" element={<DocGenuinenessComparePage />} />
                {/* <Route path="prompts" element={<PromptPage />} /> */}
              </Route>
              {/* <Route path="scenarios" element={<ScenarioPage />} /> */}
              <Route path="analytics" element={<AnalyticsPage />} />
              <Route path="chats" element={<ChatPage />} />
              <Route path="reports" element={<ReportListPage />} />
              {/* 에디터: reportId 라우팅 추가. 본문 로드/저장 API 연동은 후속(현재 localStorage). */}
              <Route path="reports/:reportId" element={<ReportEditorPage />} />
            </Route>
          </Routes>
        </AppLayout>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
