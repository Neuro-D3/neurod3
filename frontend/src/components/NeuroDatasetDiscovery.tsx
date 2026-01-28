import React, { useState, useEffect, useCallback, useRef } from 'react';
import { fetchDatasets, fetchDatasetStats } from '../services/api';

// Lightweight icon stand-ins (avoid external deps in preview)
const IconWrapper: React.FC<{ className?: string; children: React.ReactNode }> = ({
  className,
  children,
}) => (
  <span className={className} aria-hidden="true">
    {children}
  </span>
);

const ExternalLink: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>↗︎</IconWrapper>
);

const RefreshCw: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>⟳</IconWrapper>
);

const ArrowUpDown: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>⇅</IconWrapper>
);

const Info: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>ℹ︎</IconWrapper>
);

const Filter: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>☷</IconWrapper>
);

const Moon: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>☾</IconWrapper>
);

const Sun: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>☀︎</IconWrapper>
);

const Search: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>🔍</IconWrapper>
);

const ChevronUp: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>▴</IconWrapper>
);

const ChevronDown: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>▾</IconWrapper>
);

// Simple dataset type
type Dataset = {
  source: 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet';
  id: string;
  title: string;
  modality: string | null;
  tags?: string | null;
  citations: number;
  url: string;
  created_at?: string | null;
};

const SOURCE_OPTIONS = ['DANDI', 'Kaggle', 'OpenNeuro', 'PhysioNet'] as const;
const SOURCE_CANONICAL_BY_PARAM = SOURCE_OPTIONS.reduce<Record<string, Dataset['source']>>((acc, s) => {
  acc[s.toLowerCase()] = s;
  return acc;
}, {});

function isAcronymLike(token: string): boolean {
  // Heuristic: keep original casing if it contains 2+ consecutive uppercase letters (EEG, fMRI, iEEG, MRI, PET, etc.)
  return /[A-Z]{2,}/.test(token);
}

function formatModalityToken(token: string): string {
  const t = token.trim();
  if (!t) return t;
  return isAcronymLike(t) ? t : t.toLowerCase();
}

function normalizeModalityForCompare(token: string): string {
  return token.trim().toLowerCase();
}

export default function NeuroDatasetDiscovery() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [noDatasetsFound, setNoDatasetsFound] = useState<boolean>(false);
  const [sortBy, setSortBy] = useState<'published' | 'citations' | 'title' | 'id' | 'source' | 'modality'>(
    'published',
  );
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [sourceFilter, setSourceFilter] = useState<'all' | 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet'>(
    'all',
  );
  const [selectedModalities, setSelectedModalities] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [stats, setStats] = useState<{ total: number; unique: number; bySources: Record<string, number> }>({
    total: 0,
    unique: 0,
    bySources: {},
  });
  const [page, setPage] = useState<number>(1);
  const [pageSize] = useState<number>(25);
  const [totalCount, setTotalCount] = useState<number>(0);
  const [darkMode, setDarkMode] = useState<boolean>(false);
  const [availableSources, setAvailableSources] = useState<Dataset['source'][]>([...SOURCE_OPTIONS]);
  const [availableModalities, setAvailableModalities] = useState<string[]>([]);
  const [modalityDropdownOpen, setModalityDropdownOpen] = useState<boolean>(false);
  const modalityDropdownRef = useRef<HTMLDivElement | null>(null);
  const [expandedModalityRows, setExpandedModalityRows] = useState<Set<string>>(new Set());

  // Rotating placeholder examples (search is still non-functional)
  const promptExamples = [
    'How many datasets include EEG modality and were generated after 2020?',
    'Show intracranial EEG datasets with more than 100 citations.',
    'Find sleep-related EEG datasets across all repositories.',
    'List multimodal EEG + fMRI datasets with open licenses.',
  ];
  const [placeholderIndex, setPlaceholderIndex] = useState<number>(0);

  // Initialize filters from URL (?source=...&modality=...) on first render.
  useEffect(() => {
    if (typeof window === 'undefined') return;

    const params = new URLSearchParams(window.location.search);

    const sourceParam = params.get('source')?.trim();
    if (sourceParam) {
      const canonical = SOURCE_CANONICAL_BY_PARAM[sourceParam.toLowerCase()];
      if (canonical) setSourceFilter(canonical);
    }

    const modalityParam = params.get('modality')?.trim();
    if (modalityParam) {
      const parts = modalityParam
        .split(',')
        .map((p) => p.trim())
        .filter(Boolean);
      // de-dupe while preserving order
      const seen = new Set<string>();
      const uniq = parts.filter((m) => (seen.has(m) ? false : (seen.add(m), true)));
      if (uniq.length) setSelectedModalities(uniq);
    }
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % promptExamples.length);
    }, 4000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, selectedModalities]);

  // Keep URL in sync with active filters (shareable links).
  useEffect(() => {
    if (typeof window === 'undefined') return;

    const url = new URL(window.location.href);
    const params = url.searchParams;

    if (sourceFilter === 'all') params.delete('source');
    else params.set('source', sourceFilter);

    if (selectedModalities.length === 0) params.delete('modality');
    else params.set('modality', selectedModalities.join(','));

    // Avoid pushing a new history entry for each change.
    window.history.replaceState(null, '', `${url.pathname}${params.toString() ? `?${params.toString()}` : ''}${url.hash}`);
  }, [sourceFilter, selectedModalities]);

  // Close modality dropdown on outside click.
  useEffect(() => {
    if (!modalityDropdownOpen) return;
    const onMouseDown = (e: MouseEvent) => {
      const el = modalityDropdownRef.current;
      if (!el) return;
      if (e.target instanceof Node && !el.contains(e.target)) {
        setModalityDropdownOpen(false);
      }
    };
    document.addEventListener('mousedown', onMouseDown);
    return () => document.removeEventListener('mousedown', onMouseDown);
  }, [modalityDropdownOpen]);

  const fetchAllDatasets = useCallback(async () => {
    setLoading(true);
    setError(null);
    setNoDatasetsFound(false);

    try {
      const offset = (page - 1) * pageSize;
      const sourceParam = sourceFilter !== 'all' ? sourceFilter : undefined;
      const modalitiesParam = selectedModalities.length ? selectedModalities : undefined;
      // Fetch datasets from backend API
      const response = await fetchDatasets({
        source: sourceParam,
        modalities: modalitiesParam,
        limit: pageSize,
        offset,
      });
      const allDatasets: Dataset[] = response.datasets;

      // Check if datasets array is empty (connection successful but no data)
      if (!allDatasets || allDatasets.length === 0) {
        setNoDatasetsFound(true);
        setDatasets([]);
        setTotalCount(0);
        setLoading(false);
        return;
      }

      setDatasets(allDatasets);
      setTotalCount(response.count ?? 0);
      setLoading(false);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to connect to API';
      console.error('Error fetching datasets from API:', err);
      setError(`API connection error: ${errorMessage}. Please ensure the backend service is running.`);
      setNoDatasetsFound(false);
      setTotalCount(0);
      setLoading(false);
    }
  }, [page, pageSize, sourceFilter, selectedModalities]);

  useEffect(() => {
    fetchAllDatasets();
  }, [fetchAllDatasets]);

  const fetchStats = useCallback(async () => {
    try {
      const sourceParam = sourceFilter !== 'all' ? sourceFilter : undefined;
      const modalitiesParam = selectedModalities.length ? selectedModalities : undefined;

      const response = await fetchDatasetStats({ source: sourceParam, modalities: modalitiesParam });

      // Faceted options
      const bySource = response.by_source || {};
      const sources = SOURCE_OPTIONS.filter((s) => (bySource[s] || 0) > 0);
      setAvailableSources(sources as Dataset['source'][]);

      const byModality = response.by_modality || {};
      const modalities = Object.entries(byModality)
        .filter(([, count]) => Number(count) > 0)
        .map(([m]) => m)
        .sort((a, b) => a.localeCompare(b));
      setAvailableModalities(modalities);

      setStats({
        total: response.total,
        unique: response.total,
        bySources: response.by_source || {},
      });
    } catch (err) {
      console.error('Error fetching dataset stats from API:', err);
    }
  }, [sourceFilter, selectedModalities]);

  useEffect(() => {
    fetchStats();
  }, [fetchStats]);

  // If current selections become unavailable, reset to All.
  useEffect(() => {
    if (sourceFilter !== 'all' && !availableSources.includes(sourceFilter)) {
      setSourceFilter('all');
    }
  }, [availableSources, sourceFilter]);

  useEffect(() => {
    if (selectedModalities.length === 0) return;
    // Don't auto-remove selected modalities if they fall out of the
    // current available list (e.g. facets are limited, or selection yields 0).
  }, [availableModalities, selectedModalities]);

  const handleSort = (column: typeof sortBy) => {
    if (sortBy === column) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(column);
      setSortOrder('desc');
    }
  };

  const filteredDatasets = datasets.filter((ds) => {
    // Server already filters by source/modality, but keep a defensive client-side filter too.
    if (sourceFilter !== 'all' && ds.source !== sourceFilter) return false;

    if (selectedModalities.length) {
      const parts = (ds.modality || '')
        .split(/[;,]/)
        .map((p) => p.trim().toLowerCase())
        .filter(Boolean);
      const partSet = new Set(parts);
      for (const m of selectedModalities) {
        if (!partSet.has(m.toLowerCase())) return false;
      }
    }

    // NOTE: searchQuery is not yet wired to filtering on purpose
    return true;
  });

  const isSelectedModality = (token: string) => {
    const norm = normalizeModalityForCompare(token);
    return selectedModalities.some((m) => normalizeModalityForCompare(m) === norm);
  };

  const toggleRowModalities = (rowKey: string) => {
    setExpandedModalityRows((prev) => {
      const next = new Set(prev);
      if (next.has(rowKey)) next.delete(rowKey);
      else next.add(rowKey);
      return next;
    });
  };

  const toggleSelectedModality = (token: string) => {
    const canonical = formatModalityToken(token);
    if (!canonical) return;
    setSelectedModalities((prev) => {
      const canonNorm = normalizeModalityForCompare(canonical);
      if (prev.some((m) => normalizeModalityForCompare(m) === canonNorm)) {
        return prev.filter((m) => normalizeModalityForCompare(m) !== canonNorm);
      }
      return [...prev, canonical];
    });
  };

  const sortedDatasets = [...filteredDatasets].sort((a, b) => {
    let comparison = 0;
    const aData = a;
    const bData = b;

    if (sortBy === 'published') {
      const aTime = aData.created_at ? Date.parse(aData.created_at) : Number.NEGATIVE_INFINITY;
      const bTime = bData.created_at ? Date.parse(bData.created_at) : Number.NEGATIVE_INFINITY;
      comparison = (aTime || Number.NEGATIVE_INFINITY) - (bTime || Number.NEGATIVE_INFINITY);
    } else if (sortBy === 'title') comparison = aData.title.localeCompare(bData.title);
    else if (sortBy === 'id') comparison = aData.id.localeCompare(bData.id);
    else if (sortBy === 'source') comparison = aData.source.localeCompare(bData.source);
    else if (sortBy === 'modality') comparison = (aData.modality || '').localeCompare(bData.modality || '');
    else if (sortBy === 'citations') comparison = aData.citations - bData.citations;

    return sortOrder === 'asc' ? comparison : -comparison;
  });

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));
  const pageOffset = (page - 1) * pageSize;

  useEffect(() => {
    if (page > totalPages) {
      setPage(totalPages);
    }
  }, [page, totalPages]);

  const SortableHeader = ({
    column,
    children,
    className,
  }: {
    column: typeof sortBy;
    children: React.ReactNode;
    className?: string;
  }) => (
    <th
      className={`px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider cursor-pointer transition-all select-none ${
        darkMode ? 'text-gray-300 hover:bg-white/10' : 'text-gray-700 hover:bg-white/60'
      } ${className || ''}`}
      onClick={() => handleSort(column)}
    >
      <div className="flex items-center justify-center gap-2">
        {children}
        <ArrowUpDown
          size={14}
          className={sortBy === column ? 'text-blue-500' : darkMode ? 'text-gray-500' : 'text-gray-400'}
        />
        {sortBy === column && (
          <span className="text-blue-500 text-xs">{sortOrder === 'asc' ? '↑' : '↓'}</span>
        )}
      </div>
    </th>
  );

  const getSourceBadgeColor = (source: Dataset['source']) => {
    const colors: Record<Dataset['source'], string> = {
      DANDI: 'bg-purple-100 text-purple-800',
      Kaggle: 'bg-blue-100 text-blue-800',
      OpenNeuro: 'bg-green-100 text-green-800',
      PhysioNet: 'bg-orange-100 text-orange-800',
    };
    return colors[source] || 'bg-gray-100 text-gray-800';
  };

  const handleRefresh = () => {
    fetchAllDatasets();
    fetchStats();
  };

  return (
    <div
      className={`min-h-screen py-12 px-4 transition-colors duration-200 ${
        darkMode
          ? 'bg-gradient-to-br from-gray-900 via-gray-800 to-gray-900'
          : 'bg-gradient-to-br from-blue-50 via-white to-purple-50'
      }`}
    >
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="mb-8 text-center relative">
          <button
            onClick={() => setDarkMode(!darkMode)}
            className={`absolute right-0 top-0 p-3 rounded-full transition-all backdrop-blur-xl ${
              darkMode
                ? 'bg-white/10 text-yellow-300 hover:bg-white/20 shadow-lg border border-white/10'
                : 'bg-white/70 text-gray-700 hover:bg-white/90 shadow-lg border border-white/20'
            }`}
            title={darkMode ? 'Switch to Light Mode' : 'Switch to Dark Mode'}
          >
            {darkMode ? <Sun size={20} /> : <Moon size={20} />}
          </button>

          <div
            className={`inline-block backdrop-blur-xl rounded-3xl px-8 py-6 mb-6 ${
              darkMode
                ? 'bg-white/5 shadow-2xl border border-white/10'
                : 'bg-white/70 shadow-xl border border-white/20'
            }`}
          >
            <h1
              className={`text-5xl font-bold leading-tight mb-3 bg-gradient-to-r bg-clip-text text-transparent ${
                darkMode
                  ? 'from-blue-400 via-purple-400 to-pink-400'
                  : 'from-blue-600 via-purple-600 to-pink-600'
              }`}
            >
              Neuro Dataset Discovery
            </h1>
            <p className={darkMode ? 'text-lg text-gray-300' : 'text-lg text-gray-600'}>
              Explore neuroscience datasets across multiple repositories
            </p>
          </div>

          {/* Search Bar */}
          <div className="max-w-3xl mx-auto mb-8">
            <div
              className={`relative backdrop-blur-xl rounded-2xl shadow-xl border transition-all ${
                darkMode
                  ? 'bg-white/5 border-white/10 focus-within:bg-white/10 focus-within:border-white/20'
                  : 'bg-white/70 border-white/20 focus-within:bg-white/90 focus-within:border-blue-200'
              }`}
            >
              <input
                type="text"
                placeholder={searchQuery ? '' : promptExamples[placeholderIndex]}
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className={`w-full px-6 py-4 bg-transparent border-none outline-none text-base ${
                  darkMode ? 'text-white placeholder-gray-400' : 'text-gray-900 placeholder-gray-500'
                }`}
              />
              <div className="absolute right-4 top-1/2 -translate-y-1/2">
                <Search className={darkMode ? 'text-gray-400' : 'text-gray-500'} size={20} />
              </div>
            </div>
          </div>
        </div>

        {/* Stats Cards */}
        <div className="grid grid-cols-2 md:grid-cols-3 gap-4 mb-6">
          <div
            className={`rounded-2xl p-4 backdrop-blur-xl transition-all hover:scale-105 ${
              darkMode ? 'bg-white/5 shadow-xl border border-white/10' : 'bg-white/70 shadow-xl border border-white/20'
            }`}
          >
            <div className={darkMode ? 'text-2xl font-bold text-white' : 'text-2xl font-bold text-gray-900'}>
              {stats.total}
            </div>
            <div className={darkMode ? 'text-sm text-gray-300' : 'text-sm text-gray-600'}>Total Datasets</div>
          </div>
          <div
            className={`rounded-2xl p-4 backdrop-blur-xl transition-all hover:scale-105 ${
              darkMode ? 'bg-white/5 shadow-xl border border-white/10' : 'bg-white/70 shadow-xl border border-white/20'
            }`}
          >
            <div className="text-2xl font-bold bg-gradient-to-r from-purple-600 to-purple-400 bg-clip-text text-transparent">
              {stats.bySources['DANDI'] || 0}
            </div>
            <div className={darkMode ? 'text-sm text-gray-300' : 'text-sm text-gray-600'}>DANDI</div>
          </div>
          <div
            className={`rounded-2xl p-4 backdrop-blur-xl transition-all hover:scale-105 ${
              darkMode ? 'bg-white/5 shadow-xl border border-white/10' : 'bg-white/70 shadow-xl border border-white/20'
            }`}
          >
            <div className="text-2xl font-bold bg-gradient-to-r from-green-600 to-green-400 bg-clip-text text-transparent">
              {stats.bySources['OpenNeuro'] || 0}
            </div>
            <div className={darkMode ? 'text-sm text-gray-300' : 'text-sm text-gray-600'}>OpenNeuro</div>
          </div>
        </div>

        {/* Info Banner */}
        <div
          className={`rounded-2xl p-4 mb-6 flex items-start gap-3 backdrop-blur-xl ${
            darkMode
              ? 'bg-blue-500/10 border border-blue-400/20 shadow-xl'
              : 'bg-blue-50/70 border border-blue-200/50 shadow-lg'
          }`}
        >
          <Info
            className={darkMode ? 'flex-shrink-0 mt-0.5 text-blue-400' : 'flex-shrink-0 mt-0.5 text-blue-600'}
            size={20}
          />
          <div className={darkMode ? 'text-sm text-blue-100' : 'text-sm text-blue-800'}>
            <p className="font-medium mb-1">About citation counts</p>
            <p>
              Citation counts are derived from peer-reviewed publications and Google Scholar metrics for each
              dataset's primary publication. Datasets with similar titles may represent the same data on different
              platforms—click the arrow to view alternate sources.
            </p>
          </div>
        </div>

        {/* Controls */}
        <div
          className={`relative z-30 rounded-2xl shadow-xl mb-6 p-4 backdrop-blur-xl ${
            darkMode ? 'bg-white/5 border border-white/10' : 'bg-white/70 border border-white/20'
          }`}
        >
          <div className="flex flex-col md:flex-row items-center justify-center md:justify-between gap-4">
            <div className="flex items-center justify-center md:justify-start gap-4 flex-wrap">
              <div className="flex items-center gap-2">
                <Filter size={18} className={darkMode ? 'text-gray-300' : 'text-gray-600'} />
                <span className={darkMode ? 'text-sm font-medium text-gray-200' : 'text-sm font-medium text-gray-700'}>
                  Filters:
                </span>
              </div>

              <select
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value as any)}
                className={`w-44 border rounded-xl px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 backdrop-blur-xl transition-all ${
                  darkMode ? 'bg-white/10 border-white/20 text-white' : 'bg-white/80 border-gray-200 text-gray-900'
                }`}
              >
                <option value="all">All Sources</option>
                {availableSources.map((s) => (
                  <option key={s} value={s}>
                    {s}
                  </option>
                ))}
              </select>

              <div className="relative" ref={modalityDropdownRef}>
                <button
                  type="button"
                  onClick={() => setModalityDropdownOpen((v) => !v)}
                  className={`w-44 border rounded-xl px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 backdrop-blur-xl transition-all text-left ${
                    darkMode ? 'bg-white/10 border-white/20 text-white' : 'bg-white/80 border-gray-200 text-gray-900'
                  }`}
                  title={
                    selectedModalities.length
                      ? selectedModalities.map((m) => formatModalityToken(m)).join(', ')
                      : 'All Modalities'
                  }
                >
                  {selectedModalities.length === 0
                    ? 'All Modalities'
                    : selectedModalities.length === 1
                      ? formatModalityToken(selectedModalities[0])
                      : `${selectedModalities.length} selected`}
                </button>

                {modalityDropdownOpen && (
                  <div
                    className={`absolute z-50 mt-2 w-64 rounded-xl shadow-2xl border p-2 max-h-72 overflow-auto ${
                      darkMode ? 'bg-gray-900/95 border-white/10 text-gray-100' : 'bg-white border-gray-200 text-gray-900'
                    }`}
                  >
                    <button
                      type="button"
                      onClick={() => setSelectedModalities([])}
                      className={`w-full text-left px-2 py-2 rounded-lg text-sm font-medium ${
                        darkMode ? 'hover:bg-white/10' : 'hover:bg-gray-50'
                      }`}
                    >
                      All Modalities
                    </button>
                    <div className={`my-2 h-px ${darkMode ? 'bg-white/10' : 'bg-gray-200'}`} />

                    {availableModalities.length === 0 ? (
                      <div className={`px-2 py-2 text-sm ${darkMode ? 'text-gray-400' : 'text-gray-500'}`}>
                        No modalities available
                      </div>
                    ) : (
                      availableModalities.map((modality) => {
                        const checked = selectedModalities.includes(modality);
                        return (
                          <label
                            key={modality}
                            className={`flex items-center gap-2 px-2 py-1.5 rounded-lg cursor-pointer text-sm ${
                              darkMode ? 'hover:bg-white/10' : 'hover:bg-gray-50'
                            }`}
                          >
                            <input
                              type="checkbox"
                              checked={checked}
                              onChange={() => {
                                setSelectedModalities((prev) =>
                                  prev.includes(modality)
                                    ? prev.filter((m) => m !== modality)
                                    : [...prev, modality],
                                );
                              }}
                            />
                            <span>{formatModalityToken(modality)}</span>
                          </label>
                        );
                      })
                    )}
                  </div>
                )}
              </div>

              <div className={darkMode ? 'text-sm text-gray-300 text-center' : 'text-sm text-gray-600 text-center'}>
                Showing{' '}
                <span className={darkMode ? 'font-semibold text-white' : 'font-semibold text-gray-900'}>
                  {sortedDatasets.length}
                </span>{' '}
                of{' '}
                <span className={darkMode ? 'font-semibold text-white' : 'font-semibold text-gray-900'}>
                  {totalCount}
                </span>{' '}
                datasets
              </div>
            </div>

            <button
              onClick={handleRefresh}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-blue-600 to-blue-500 text-white rounded-xl border border-white/10 shadow-xl hover:from-blue-500 hover:to-blue-400 disabled:from-gray-500 disabled:to-gray-400 disabled:opacity-60 disabled:cursor-not-allowed backdrop-blur-xl transition-all text-sm font-medium"
            >
              <RefreshCw size={16} className={loading ? 'animate-spin' : ''} />
              {loading ? 'Loading...' : 'Refresh'}
            </button>
          </div>
        </div>

        {/* Table */}
        {error ? (
          <div
            className={
              darkMode
                ? 'rounded-2xl shadow-xl p-12 text-center backdrop-blur-xl bg-red-900/20 border border-red-500/30'
                : 'rounded-2xl shadow-xl p-12 text-center backdrop-blur-xl bg-red-50 border border-red-200'
            }
          >
            <div className="text-red-600 mb-4 text-5xl">⚠️</div>
            <h3 className={darkMode ? 'text-red-400 font-semibold mb-2' : 'text-red-700 font-semibold mb-2'}>
              Connection Error
            </h3>
            <p className={darkMode ? 'text-gray-300 mb-4' : 'text-gray-600 mb-4'}>
              {error}
            </p>
            <button
              onClick={handleRefresh}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              Retry Connection
            </button>
          </div>
        ) : noDatasetsFound ? (
          <div
            className={
              darkMode
                ? 'rounded-2xl shadow-xl p-12 text-center backdrop-blur-xl bg-yellow-900/20 border border-yellow-500/30'
                : 'rounded-2xl shadow-xl p-12 text-center backdrop-blur-xl bg-yellow-50 border border-yellow-200'
            }
          >
            <div className="text-yellow-600 mb-4 text-5xl">📭</div>
            <h3 className={darkMode ? 'text-yellow-400 font-semibold mb-2' : 'text-yellow-700 font-semibold mb-2'}>
              No Datasets Found
            </h3>
            <p className={darkMode ? 'text-gray-300 mb-4' : 'text-gray-600 mb-4'}>
              No datasets match the current filters, or the database may be empty.
            </p>
            <button
              onClick={handleRefresh}
              className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
            >
              Retry Connection
            </button>
          </div>
        ) : loading ? (
          <div
            className={
              darkMode
                ? 'rounded-2xl shadow-xl p-12 text-center backdrop-blur-xl bg-white/5 border border-white/10'
                : 'rounded-2xl shadow-xl p-12 text-center backdrop-blur-xl bg-white/70 border border-white/20'
            }
          >
            <div className="inline-block animate-spin rounded-full h-10 w-10 border-b-2 border-blue-600 mb-4"></div>
            <p className={darkMode ? 'text-gray-300' : 'text-gray-600'}>
              Loading datasets from database...
            </p>
          </div>
        ) : (
          <div
            className={
              darkMode
                ? 'rounded-2xl shadow-2xl overflow-hidden backdrop-blur-xl bg-white/5 border border-white/10'
                : 'rounded-2xl shadow-2xl overflow-hidden backdrop-blur-xl bg-white/70 border border-white/20'
            }
          >
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className={darkMode ? 'backdrop-blur-xl bg-white/5' : 'backdrop-blur-xl bg-white/50'}>
                  <tr>
                    <th
                      className={
                        darkMode
                          ? 'px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-300'
                          : 'px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-700'
                      }
                    >
                      #
                    </th>
                    <SortableHeader column="source">Source</SortableHeader>
                    <SortableHeader column="title" className="w-[34rem]">Dataset Title</SortableHeader>
                    <SortableHeader column="id">ID</SortableHeader>
                    <SortableHeader column="published">Published</SortableHeader>
                    <SortableHeader column="modality">Modality</SortableHeader>
                    <SortableHeader column="citations">Citations</SortableHeader>
                    <th
                      className={
                        darkMode
                          ? 'px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-300'
                          : 'px-4 py-3 text-center text-xs font-semibold uppercase tracking-wider text-gray-700'
                      }
                    >
                      Links
                    </th>
                  </tr>
                </thead>
                <tbody className={darkMode ? 'divide-y divide-gray-800' : 'divide-y divide-gray-200'}>
                  {sortedDatasets.map((ds, index) => (
                    <tr key={`${ds.source}-${ds.id}-${index}`} className={darkMode ? 'hover:bg-white/5' : 'hover:bg-gray-50'}>
                      <td className="px-4 py-3 text-sm text-center text-gray-800">{pageOffset + index + 1}</td>
                      <td className="px-4 py-3 text-sm text-center">
                        <button
                          type="button"
                          onClick={() => setSourceFilter((prev) => (prev === ds.source ? 'all' : ds.source))}
                          className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium transition-all ${
                            sourceFilter !== 'all' && sourceFilter === ds.source ? 'ring-1 ring-blue-500/40 shadow-sm' : ''
                          } ${getSourceBadgeColor(ds.source)}`}
                          title="Click to toggle filter"
                        >
                          {ds.source}
                        </button>
                          </td>
                          <td
                            className="px-4 py-3 text-sm font-medium text-center"
                            style={{ color: darkMode ? '#F9FAFB' : '#111827' }}
                          >
                            {ds.title}
                          </td>
                          <td
                            className="px-4 py-3 text-sm text-center"
                            style={{ color: darkMode ? '#E5E7EB' : '#111827' }}
                          >
                            {ds.id}
                          </td>
                          <td
                            className="px-4 py-3 text-sm text-center"
                            style={{ color: darkMode ? '#E5E7EB' : '#111827' }}
                          >
                            <div className="flex justify-center">
                              <span className="whitespace-nowrap tabular-nums">
                                {ds.created_at ? new Date(ds.created_at).toLocaleDateString() : '—'}
                              </span>
                            </div>
                          </td>
                          <td
                            className="px-4 py-3 text-sm text-center"
                            style={{ color: darkMode ? '#E5E7EB' : '#111827' }}
                          >
                            {(() => {
                              const rowKey = `${ds.source}:${ds.id}`;
                              const parts = (ds.modality || '')
                                .split(/[;,]/)
                                .map((p) => p.trim())
                                .filter(Boolean);
                              if (parts.length === 0) return '—';
                              const isExpanded = expandedModalityRows.has(rowKey);
                              const previewCount = 3;
                              const visible = isExpanded ? parts : parts.slice(0, previewCount);
                              const remaining = parts.length - visible.length;
                              return (
                                <div className="flex flex-wrap items-center justify-center gap-1">
                                  {visible.map((m) => (
                                    <button
                                      type="button"
                                      onClick={() => toggleSelectedModality(m)}
                                      key={m}
                                      className={`px-2 py-0.5 rounded-full text-[11px] font-medium transition-all cursor-pointer ${
                                        isSelectedModality(m)
                                          ? 'text-sky-200 bg-gradient-to-r from-slate-950/80 via-blue-950/70 to-blue-900/60 shadow-md shadow-blue-900/40 ring-1 ring-blue-400/25'
                                          : darkMode
                                            ? 'bg-white/10 text-gray-200'
                                            : 'bg-gray-100 text-gray-700'
                                      }`}
                                      title="Click to toggle filter"
                                    >
                                      {formatModalityToken(m)}
                                    </button>
                                  ))}

                                  {parts.length > previewCount && (
                                    <button
                                      type="button"
                                      onClick={() => toggleRowModalities(rowKey)}
                                      className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-[11px] font-semibold transition-all ${
                                        darkMode
                                          ? 'bg-white/5 text-gray-200 hover:bg-white/10 border border-white/10'
                                          : 'bg-white/70 text-gray-800 hover:bg-white border border-gray-200'
                                      }`}
                                      title={isExpanded ? 'Collapse modalities' : 'Expand modalities'}
                                    >
                                      {isExpanded ? <ChevronUp /> : <ChevronDown />}
                                      {!isExpanded && remaining > 0 ? `+${remaining}` : 'Less'}
                                    </button>
                                  )}
                                </div>
                              );
                            })()}
                          </td>
                          <td
                            className="px-4 py-3 text-sm font-semibold text-center"
                            style={{ color: darkMode ? '#F9FAFB' : '#111827' }}
                          >
                            {ds.citations.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 text-sm text-center">
                            <a
                              href={ds.url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center justify-center gap-1 text-blue-600 hover:text-blue-500"
                            >
                              Open
                              <ExternalLink size={14} />
                            </a>
                          </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            <div
              className={`flex items-center justify-between px-4 py-3 border-t ${
                darkMode ? 'border-white/10 text-gray-300' : 'border-gray-200 text-gray-600'
              }`}
            >
              <div className="text-sm">
                Page{' '}
                <span className={darkMode ? 'font-semibold text-white' : 'font-semibold text-gray-900'}>
                  {page}
                </span>{' '}
                of{' '}
                <span className={darkMode ? 'font-semibold text-white' : 'font-semibold text-gray-900'}>
                  {totalPages}
                </span>
              </div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                  disabled={page <= 1 || loading}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    darkMode
                      ? 'bg-white/5 border border-white/10 text-gray-200 hover:bg-white/10 disabled:opacity-40'
                      : 'bg-white border border-gray-200 text-gray-700 hover:bg-gray-50 disabled:opacity-40'
                  }`}
                >
                  Previous
                </button>
                <button
                  onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                  disabled={page >= totalPages || loading}
                  className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                    darkMode
                      ? 'bg-white/5 border border-white/10 text-gray-200 hover:bg-white/10 disabled:opacity-40'
                      : 'bg-white border border-gray-200 text-gray-700 hover:bg-gray-50 disabled:opacity-40'
                  }`}
                >
                  Next
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Footer */}
        <div
          className={`mt-6 text-center text-sm backdrop-blur-xl rounded-2xl p-4 ${
            darkMode
              ? 'text-gray-300 bg-white/5 border border-white/10'
              : 'text-gray-600 bg-white/70 border border-white/20'
          }`}
        >
          <p className="mb-2">
            Citation data sourced from Google Scholar and published literature
          </p>
          <div className="flex justify-center gap-4 flex-wrap">
            <a
              href="https://dandiarchive.org"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:text-blue-400 transition-colors"
            >
              DANDI Archive
            </a>
            <a
              href="https://kaggle.com/datasets"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:text-blue-400 transition-colors"
            >
              Kaggle Datasets
            </a>
            <a
              href="https://openneuro.org"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:text-blue-400 transition-colors"
            >
              OpenNeuro
            </a>
            <a
              href="https://physionet.org"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:text-blue-400 transition-colors"
            >
              PhysioNet
            </a>
          </div>
          <p className="mt-3 text-xs opacity-75">
            Developed by{' '}
            <a
              href="https://foresight.org/"
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:text-blue-400 transition-colors"
            >
              Foresight Institute
            </a>
          </p>
        </div>
      </div>
    </div>
  );
}


