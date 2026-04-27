import {
  BrowserRouter,
  Navigate,
  Route,
  Routes,
} from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ChatPage } from "./pages/ChatPage";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProjectLayout from "./layout/ProjectLayout";
import DatasetPage from "./pages/DatasetPage";
import ScenarioPage from "./routes/ScenarioPage";
import DatasetVersionPage from "./features/dataset/pages/DatasetVersionPage";
import ProjectPage from "./pages/ProjectPage";

function App() {
  const queryClient = new QueryClient();
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <AppLayout>
          <Routes>
            <Route path="/" element={<Navigate to="/chats" replace />} />
            <Route path="/chats" element={<ChatPage />} />
            <Route path="/projects" element={<ProjectPage />} />
            <Route path="/projects/:projectId" element={<ProjectLayout />}>
              <Route index  element={<Navigate to="datasets" replace />} />
              <Route path="datasets" element={<DatasetPage />} />
              <Route path="datasets/:datasetId">
              <Route index element={<Navigate to="versions" replace />} />
              <Route path="versions" element={<DatasetVersionPage />} />
              </Route>
              <Route path="scenarios" element={<ScenarioPage />} />
            </Route>
          </Routes>
        </AppLayout>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App;
