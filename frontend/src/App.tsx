import React, { useLayoutEffect } from 'react';
import { BrowserRouter, Routes, Route, useLocation, useNavigate, Navigate } from 'react-router-dom';
import './App.css';
import NeuroDatasetDiscovery from './components/NeuroDatasetDiscovery';
import PaperMappingDashboard from './pages/PaperMappingDashboard';
import DatasetDetailPage from './pages/DatasetDetailPage';

function scrollWindowToTop() {
  window.scrollTo(0, 0);
  document.documentElement.scrollTop = 0;
  document.body.scrollTop = 0;
}

/** Only `/datasets/:source/:id` — scroll the feed when returning home is left unchanged. */
function isDatasetDetailPath(pathname: string): boolean {
  return /^\/datasets\/[^/]+\/[^/]+$/.test(pathname);
}

function ScrollToTop() {
  const { pathname } = useLocation();
  useLayoutEffect(() => {
    if (!isDatasetDetailPath(pathname)) return;
    scrollWindowToTop();
    const id = window.requestAnimationFrame(() => {
      scrollWindowToTop();
    });
    return () => window.cancelAnimationFrame(id);
  }, [pathname]);
  return null;
}

function AppLayout() {
  const location = useLocation();
  const navigate = useNavigate();

  const isDetailPage = location.pathname.startsWith('/datasets/') && location.pathname !== '/datasets/';
  const isDatasets = location.pathname === '/' || location.pathname === '/datasets';
  const isPaperMapping = location.pathname === '/paper-mapping';

  return (
    <div className="App">
      <ScrollToTop />
      <div className="sticky top-0 z-40 border-b border-slate-200 bg-white/90 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3 sm:px-6 lg:px-8">
          <button
            type="button"
            onClick={() => navigate('/')}
            className="text-left hover:opacity-80 transition-opacity"
          >
            <div className="text-lg font-semibold text-slate-900">NeuroD3</div>
            <div className="text-sm text-slate-500">Dataset discovery and internal paper mapping</div>
          </button>
          {!isDetailPage && (
            <div className="flex items-center gap-2 rounded-full bg-slate-100 p-1">
              <button
                type="button"
                onClick={() => navigate('/')}
                className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                  isDatasets ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                Datasets
              </button>
              <button
                type="button"
                onClick={() => navigate('/paper-mapping')}
                className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                  isPaperMapping ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                Paper Mapping
              </button>
            </div>
          )}
        </div>
      </div>

      <Routes>
        <Route path="/" element={<NeuroDatasetDiscovery />} />
        <Route path="/paper-mapping" element={<PaperMappingDashboard />} />
        <Route path="/datasets/:source/:datasetId" element={<DatasetDetailPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  );
}

/** Derive basename from the URL for PR preview environments (e.g. /pr-62/app -> /pr-62/app). */
function getBasename(): string {
  if (typeof window === 'undefined') return '/';
  const m = window.location.pathname.match(/^(\/pr-\d+\/app)/);
  return m?.[1] ?? '/';
}

function App() {
  return (
    <BrowserRouter basename={getBasename()} unstable_useTransitions={false}>
      <AppLayout />
    </BrowserRouter>
  );
}

export default App;
