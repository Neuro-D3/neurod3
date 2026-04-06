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

/** Only `/datasets/:id` — scroll the feed when returning home is left unchanged. */
function isDatasetDetailPath(pathname: string): boolean {
  return /^\/datasets\/[^/]+$/.test(pathname);
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
          <div className="flex items-center gap-3">
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
            <a
              href="https://github.com/Neuro-D3/neurod3"
              target="_blank"
              rel="noopener noreferrer"
              aria-label="View source on GitHub"
              className="rounded-full p-2 text-slate-500 hover:text-slate-900 transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-slate-400"
            >
              <svg
                aria-hidden="true"
                viewBox="0 0 16 16"
                className="h-5 w-5 fill-current"
              >
                <path d="M8 0C3.58 0 0 3.58 0 8c0 3.54 2.29 6.53 5.47 7.59.4.07.55-.17.55-.38 0-.19-.01-.82-.01-1.49-2.01.37-2.53-.49-2.69-.94-.09-.23-.48-.94-.82-1.13-.28-.15-.68-.52-.01-.53.63-.01 1.08.58 1.23.82.72 1.21 1.87.87 2.33.66.07-.52.28-.87.51-1.07-1.78-.2-3.64-.89-3.64-3.95 0-.87.31-1.59.82-2.15-.08-.2-.36-1.02.08-2.12 0 0 .67-.21 2.2.82.64-.18 1.32-.27 2-.27.68 0 1.36.09 2 .27 1.53-1.04 2.2-.82 2.2-.82.44 1.1.16 1.92.08 2.12.51.56.82 1.27.82 2.15 0 3.07-1.87 3.75-3.65 3.95.29.25.54.73.54 1.48 0 1.07-.01 1.93-.01 2.2 0 .21.15.46.55.38A8.013 8.013 0 0016 8c0-4.42-3.58-8-8-8z" />
              </svg>
            </a>
          </div>
        </div>
      </div>

      <Routes>
        <Route path="/" element={<NeuroDatasetDiscovery />} />
        <Route path="/paper-mapping" element={<PaperMappingDashboard />} />
        <Route path="/datasets/:datasetId" element={<DatasetDetailPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter unstable_useTransitions={false}>
      <AppLayout />
    </BrowserRouter>
  );
}

export default App;
