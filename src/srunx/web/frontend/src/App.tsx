import { Route, Routes } from "react-router-dom";
import { ErrorBoundary } from "./components/ErrorBoundary.tsx";
import { Layout } from "./components/Layout.tsx";
import { Dashboard } from "./pages/Dashboard.tsx";
import { Jobs } from "./pages/Jobs.tsx";
import { Workflows } from "./pages/Workflows.tsx";
import { WorkflowDetail } from "./pages/WorkflowDetail.tsx";
import { Resources } from "./pages/Resources.tsx";
import { LogViewer } from "./pages/LogViewer.tsx";

export function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="jobs" element={<Jobs />} />
          <Route path="jobs/:jobId/logs" element={<LogViewer />} />
          <Route path="workflows" element={<Workflows />} />
          <Route path="workflows/:name" element={<WorkflowDetail />} />
          <Route path="resources" element={<Resources />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
