import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ChatPage } from "./features/chats/pages/ChatPage";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProjectLayout from "./layout/ProjectLayout";
import ProjectPage from "./pages/ProjectPage";
// import DatasetDetail from "./features/datasets/pages/DatasetDetail"; // 리디자인으로 교체

import DatasetContainer from "./features/datasets/pages/DatasetContainer";
import VersionDetailPage from "./features/versions/pages/VersionDetailPage";
import DatasetVersionListRedesign from "./features/versions/redesign/DatasetVersionListRedesign";

const queryClient = new QueryClient();

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppLayout>
          <Routes>
            <Route path="/" element={<Navigate to="/projects" replace />} />
            <Route path="/projects" element={<ProjectPage />} />
            <Route path="/projects/:projectId" element={<ProjectLayout />}>
              <Route index element={<Navigate to="datasets" replace />} />
              <Route path="datasets" element={<DatasetContainer />} />
              <Route path="datasets/:datasetId">
                <Route path="versions/:versionId" element={<VersionDetailPage /> }/> 
                <Route index element={<Navigate to="versions" replace />} />
                {/* 기존 버전 목록 화면 (리디자인으로 교체, 보존) */}
                {/* <Route path="versions" element={<DatasetDetail />} /> */}
                <Route path="versions" element={<DatasetVersionListRedesign />} />
                {/* <Route path="prompts" element={<PromptPage />} /> */}
              </Route>
              {/* <Route path="scenarios" element={<ScenarioPage />} /> */}
              <Route path="chats" element={<ChatPage />} />
            </Route>
          </Routes>
        </AppLayout>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
