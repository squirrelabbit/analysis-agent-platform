import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import AppLayout from "./layout/AppLayout";
import { ChatPage } from "./pages/ChatPage";
import ProjectsPage from "./routes/ProjectsPage";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import ProjectPage from './pages/ProjectPage'

function App() {
  const apiBaseUrl =
    import.meta.env.VITE_API_BASE_URL?.trim() || "http://127.0.0.1:18080";

  const queryClient = new QueryClient();
  return (
    <BrowserRouter>
      <QueryClientProvider client={queryClient}>
        <AppLayout>
          <Routes>
            <Route path="/projects" element={<ProjectsPage />} />
            <Route path="/projects/:id" element={<ProjectsPage />} />
            <Route path="/" element={<Navigate to="/chat" replace />} />
            <Route path="/chat" element={<ChatPage />} />
            <Route path="/project" element={<ProjectPage />} />
          </Routes>
        </AppLayout>
      </QueryClientProvider>
    </BrowserRouter>
  );
}

export default App;
