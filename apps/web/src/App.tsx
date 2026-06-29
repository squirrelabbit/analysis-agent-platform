import {
  createBrowserRouter,
  createRoutesFromElements,
  Navigate,
  Route,
  RouterProvider,
} from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ChatPage } from "./features/chats/pages/ChatPage";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProjectLayout from "./layout/ProjectLayout";
import ProjectPage from "./pages/ProjectPage";
import DatasetListRedesign from "./features/datasets/redesign/DatasetListRedesign";
import VersionDetailPage from "./features/versions/pages/VersionDetailPage";
import DatasetVersionListRedesign from "./features/versions/redesign/DatasetVersionListRedesign";
import DocGenuinenessComparePage from "./features/versions/pages/DocGenuinenessComparePage";
import LoginPage from "./features/auth/pages/LoginPage";
import ReportListPage from "./features/reports/pages/ReportListPage";
import ReportEditorPage from "./features/reports/pages/ReportEditorPage";

const queryClient = new QueryClient();

// useBlocker(미저장 보고서 편집 경고 등)는 data router에서만 동작하므로
// createBrowserRouter를 쓴다. 라우트 트리는 기존 JSX 그대로 유지하고, AppLayout을
// 루트 레이아웃 라우트(Outlet)로 감싼다.
const router = createBrowserRouter(
  createRoutesFromElements(
    <Route element={<AppLayout />}>
      <Route path="/login" element={<LoginPage />} />
      <Route path="/" element={<Navigate to="/projects" replace />} />
      <Route path="/projects" element={<ProjectPage />} />
      <Route path="/projects/:projectId" element={<ProjectLayout />}>
        <Route index element={<Navigate to="datasets" replace />} />
        {/* 기존 데이터셋 목록 화면 (리디자인으로 교체, 보존) */}
        {/* <Route path="datasets" element={<DatasetContainer />} /> */}
        <Route path="datasets" element={<DatasetListRedesign />} />
        <Route path="datasets/:datasetId">
          <Route path="versions/:versionId" element={<VersionDetailPage />} />
          <Route index element={<Navigate to="versions" replace />} />
          {/* 기존 버전 목록 화면 (리디자인으로 교체, 보존) */}
          {/* <Route path="versions" element={<DatasetDetail />} /> */}
          <Route path="versions" element={<DatasetVersionListRedesign />} />
          <Route
            path="doc-genuineness-compare"
            element={<DocGenuinenessComparePage />}
          />
        </Route>
        <Route path="chats" element={<ChatPage />} />
        <Route path="reports" element={<ReportListPage />} />
        {/* 에디터: reportId 라우팅 추가. */}
        <Route path="reports/:reportId" element={<ReportEditorPage />} />
      </Route>
    </Route>,
  ),
);

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <RouterProvider router={router} />
    </QueryClientProvider>
  );
}

export default App;
