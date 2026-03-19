import React, { useEffect, useState } from 'react';
import './App.css';
import NeuroDatasetDiscovery from './components/NeuroDatasetDiscovery';
import PaperMappingDashboard from './pages/PaperMappingDashboard';

type AppTab = 'datasets' | 'paper-mapping';

function App() {
  const [tab, setTab] = useState<AppTab>('datasets');

  useEffect(() => {
    if (typeof window === 'undefined') return;
    if (window.location.hash === '#paper-mapping') {
      setTab('paper-mapping');
    }
  }, []);

  useEffect(() => {
    if (typeof window === 'undefined') return;
    window.history.replaceState(null, '', tab === 'paper-mapping' ? '#paper-mapping' : '#datasets');
  }, [tab]);

  return (
    <div className="App">
      <div className="sticky top-0 z-30 border-b border-slate-200 bg-white/90 backdrop-blur">
        <div className="mx-auto flex max-w-7xl items-center justify-between px-4 py-3 sm:px-6 lg:px-8">
          <div>
            <div className="text-lg font-semibold text-slate-900">NeuroD3</div>
            <div className="text-sm text-slate-500">Dataset discovery and internal paper mapping</div>
          </div>
          <div className="flex items-center gap-2 rounded-full bg-slate-100 p-1">
            <button
              type="button"
              onClick={() => setTab('datasets')}
              className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                tab === 'datasets' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Datasets
            </button>
            <button
              type="button"
              onClick={() => setTab('paper-mapping')}
              className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                tab === 'paper-mapping' ? 'bg-white text-slate-900 shadow-sm' : 'text-slate-600 hover:text-slate-900'
              }`}
            >
              Paper Mapping
            </button>
          </div>
        </div>
      </div>
      {tab === 'datasets' ? <NeuroDatasetDiscovery /> : <PaperMappingDashboard />}
    </div>
  );
}

export default App;

