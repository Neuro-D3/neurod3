import React, { useLayoutEffect } from 'react';
import { BrowserRouter, Routes, Route, useLocation, Navigate, Link } from 'react-router-dom';
import './App.css';
import NeuroDatasetDiscovery from './components/NeuroDatasetDiscovery';
import PaperMappingDashboard from './pages/PaperMappingDashboard';
import DatasetDetailPage from './pages/DatasetDetailPage';
import { SiteFooter } from './components/SiteFooter';
import { cx } from './components/ui';

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

/**
 * The navy masthead on every page: the brand on the left, the two sections
 * on the right. A dataset page continues the navy below it with its own
 * header, so the bar's bottom edge is a hairline rather than a shadow.
 */
function MastheadNav() {
  const location = useLocation();
  const isDetailPage = isDatasetDetailPath(location.pathname);
  const isDatasets = !isDetailPage && (location.pathname === '/' || location.pathname === '/datasets');
  const isPaperMapping = location.pathname === '/paper-mapping';

  const item = (active: boolean) =>
    cx(
      'flex h-12 items-center border-b-[3px] px-1 text-sm no-underline hover:no-underline',
      active ? 'border-white font-bold text-white' : 'border-transparent text-navy-sub hover:text-white',
    );

  return (
    <header className={cx('bg-navy text-white', isDetailPage ? 'border-b border-navy-line' : '')}>
      <div className="mx-auto flex max-w-[1440px] items-center justify-between gap-6 px-4 sm:px-6 lg:px-8">
        <Link to="/" className="flex items-baseline gap-3 py-2.5 no-underline hover:no-underline">
          <span className="text-lg font-bold tracking-tight text-white">NeuroD3</span>
          <span className="hidden font-mono text-xs uppercase tracking-[0.08em] text-navy-muted sm:inline">
            Dataset Reuse Hub
          </span>
        </Link>
        <nav aria-label="Sections" className="flex items-center gap-6">
          <Link to="/" aria-current={isDatasets ? 'page' : undefined} className={item(isDatasets)}>
            Datasets
          </Link>
          <Link to="/paper-mapping" aria-current={isPaperMapping ? 'page' : undefined} className={item(isPaperMapping)}>
            Paper Mapping
          </Link>
        </nav>
      </div>
    </header>
  );
}

function AppLayout() {
  return (
    <div className="App">
      <ScrollToTop />
      <MastheadNav />
      <main>
        <Routes>
          <Route path="/" element={<NeuroDatasetDiscovery />} />
          <Route path="/paper-mapping" element={<PaperMappingDashboard />} />
          <Route path="/datasets/:source/:datasetId" element={<DatasetDetailPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </main>
      <SiteFooter />
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
