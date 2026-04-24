import { Route, Routes } from "react-router-dom";
import { ErrorBoundary } from "./components/ErrorBoundary.tsx";
import { Layout } from "./components/Layout.tsx";
import { Dashboard } from "./pages/Dashboard.tsx";
import { Jobs } from "./pages/Jobs.tsx";
import { Workflows } from "./pages/Workflows.tsx";
import { WorkflowDetail } from "./pages/WorkflowDetail.tsx";
import { WorkflowBuilder } from "./pages/WorkflowBuilder.tsx";
import { Templates } from "./pages/Templates.tsx";
import { Resources } from "./pages/Resources.tsx";
import { Settings } from "./pages/Settings.tsx";
import { JobDetail } from "./pages/JobDetail.tsx";
import { NotificationsCenter } from "./pages/NotificationsCenter.tsx";
import { SweepRunsPage } from "./pages/SweepRunsPage.tsx";
import { SweepRunDetailPage } from "./pages/SweepRunDetailPage.tsx";
import { WorkflowRunStandalonePage } from "./pages/WorkflowRunStandalonePage.tsx";

export function App() {
  return (
    <ErrorBoundary>
      <Routes>
        <Route element={<Layout />}>
          <Route index element={<Dashboard />} />
          <Route path="jobs" element={<Jobs />} />
          <Route path="jobs/:jobId" element={<JobDetail />} />
          <Route path="jobs/:jobId/logs" element={<JobDetail />} />
          <Route path="jobs/:jobId/notifications" element={<JobDetail />} />
          <Route path="workflows" element={<Workflows />} />
          <Route path="workflows/new" element={<WorkflowBuilder />} />
          <Route path="workflows/:name/edit" element={<WorkflowBuilder />} />
          <Route path="workflows/:name" element={<WorkflowDetail />} />
          <Route path="sweep_runs" element={<SweepRunsPage />} />
          <Route path="sweep_runs/:id" element={<SweepRunDetailPage />} />
          <Route
            path="workflow_runs/:id"
            element={<WorkflowRunStandalonePage />}
          />
          <Route path="templates" element={<Templates />} />
          <Route path="resources" element={<Resources />} />
          <Route path="notifications" element={<NotificationsCenter />} />
          <Route path="settings" element={<Settings />} />
        </Route>
      </Routes>
    </ErrorBoundary>
  );
}
