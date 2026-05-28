import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ChatPage } from "./pages/ChatPage";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProjectLayout from "./layout/ProjectLayout";
import ProjectPage from "./pages/ProjectPage";
import DatasetDetail from "./features/datasets/pages/DatasetDetail";
import DatasetContainer from "./features/datasets/pages/DatasetContainer";
import VersionDetailPage from "./features/versions/pages/VersionDetailPage";

function App() {
  const queryClient = new QueryClient();
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
                <Route path="versions" element={<DatasetDetail />} />
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
