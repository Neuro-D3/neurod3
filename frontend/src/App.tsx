import React, { useEffect, useState } from 'react';
import { BrowserRouter, Routes, Route, useLocation, useNavigate } from 'react-router-dom';
import './App.css';
import NeuroDatasetDiscovery from './components/NeuroDatasetDiscovery';
import PaperMappingDashboard from './pages/PaperMappingDashboard';
import DatasetDetailPage from './pages/DatasetDetailPage';

type AppTab = 'datasets' | 'paper-mapping';

function AppLayout() {
  const location = useLocation();
  const navigate = useNavigate();

  const isDetailPage = location.pathname.startsWith('/datasets/') && location.pathname !== '/datasets/';
  const tabFromPath: AppTab = location.hash === '#paper-mapping' ? 'paper-mapping' : 'datasets';
  const [tab, setTab] = useState<AppTab>(tabFromPath);

  useEffect(() => {
    if (isDetailPage) return;
    const newTab: AppTab = location.hash === '#paper-mapping' ? 'paper-mapping' : 'datasets';
    setTab(newTab);
  }, [location.hash, isDetailPage]);

  const switchTab = (t: AppTab) => {
    setTab(t);
    navigate(t === 'paper-mapping' ? '/#paper-mapping' : '/#datasets', { replace: true });
  };

  return (
    <div className="App">
      <div className="sticky top-0 z-30 border-b border-slate-200 bg-white/90 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3 sm:px-6 lg:px-8">
          <button
            type="button"
            onClick={() => switchTab('datasets')}
            className="text-left hover:opacity-80 transition-opacity"
          >
            <div className="text-lg font-semibold text-slate-900">NeuroD3</div>
            <div className="text-sm text-slate-500">Dataset discovery and internal paper mapping</div>
          </button>
          {!isDetailPage && (
            <div className="flex items-center gap-2 rounded-full bg-slate-100 p-1">
              <button
                type="button"
                onClick={() => switchTab('datasets')}
                className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                  tab === 'datasets' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                Datasets
              </button>
              <button
                type="button"
                onClick={() => switchTab('paper-mapping')}
                className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                  tab === 'paper-mapping' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
                }`}
              >
                Paper Mapping
              </button>
            </div>
          )}
        </div>
      </div>

      <Routes>
        <Route path="/datasets/:datasetId" element={<DatasetDetailPage />} />
        <Route
          path="*"
          element={tab === 'paper-mapping' ? <PaperMappingDashboard /> : <NeuroDatasetDiscovery />}
        />
      </Routes>
    </div>
  );
}

function App() {
  return (
    <BrowserRouter>
      <AppLayout />
    </BrowserRouter>
  );
}

export default App;
