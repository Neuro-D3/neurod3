import React, { useState, useEffect } from 'react';
import { fetchDatasets } from '../services/api';

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

const ChevronDown: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>▾</IconWrapper>
);

const ChevronRight: React.FC<{ size?: number; className?: string }> = ({ className }) => (
  <IconWrapper className={className}>▸</IconWrapper>
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

// Simple dataset type
type Dataset = {
  source: 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet';
  id: string;
  title: string;
  modality: string;
  citations: number;
  url: string;
};

// Grouped version with potential duplicates
type DatasetGroup = {
  primary: Dataset;
  alternates: Dataset[];
  hasDuplicates: boolean;
};

export default function NeuroDatasetDiscovery() {
  const [datasets, setDatasets] = useState<DatasetGroup[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<'citations' | 'title' | 'id' | 'source' | 'modality'>(
    'citations',
  );
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [sourceFilter, setSourceFilter] = useState<'all' | 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet'>(
    'all',
  );
  const [modalityFilter, setModalityFilter] = useState<string>('all');
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [expandedRows, setExpandedRows] = useState<Set<number>>(new Set());
  const [stats, setStats] = useState<{ total: number; unique: number; bySources: Record<string, number> }>({
    total: 0,
    unique: 0,
    bySources: {},
  });
  const [darkMode, setDarkMode] = useState<boolean>(false);
  const [allModalities, setAllModalities] = useState<string[]>([]);

  // Rotating placeholder examples (search is still non-functional)
  const promptExamples = [
    'How many datasets include EEG modality and were generated after 2020?',
    'Show intracranial EEG datasets with more than 100 citations.',
    'Find sleep-related EEG datasets across all repositories.',
    'List multimodal EEG + fMRI datasets with open licenses.',
  ];
  const [placeholderIndex, setPlaceholderIndex] = useState<number>(0);

  useEffect(() => {
    fetchAllDatasets();
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % promptExamples.length);
    }, 4000);
    return () => clearInterval(interval);
  }, []);

  const fetchAllDatasets = async () => {
    setLoading(true);
    setError(null);

    try {
      // Fetch datasets from backend API
      const response = await fetchDatasets();
      const allDatasets: Dataset[] = response.datasets;

      const groupedDatasets = groupDuplicates(allDatasets);

      const modalitiesSet = new Set<string>();
      allDatasets.forEach((ds) => {
        ds.modality.split(',').forEach((mod) => {
          const cleaned = mod.trim();
          if (cleaned) modalitiesSet.add(cleaned);
        });
      });
      setAllModalities(Array.from(modalitiesSet).sort());

      setDatasets(groupedDatasets);
      calculateStats(allDatasets, groupedDatasets);
      setLoading(false);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Failed to connect to database';
      console.error('Error fetching datasets from database:', err);
      setError(`Database connection error: ${errorMessage}. Please ensure the API server is running.`);
      setLoading(false);
    }
  };

  const groupDuplicates = (all: Dataset[]): DatasetGroup[] => {
    const groups: DatasetGroup[] = [];
    const processed = new Set<number>();

    all.forEach((dataset, index) => {
      if (processed.has(index)) return;

      const normalizedTitle = dataset.title
        .toLowerCase()
        .replace(/[^a-z0-9\s]/g, '')
        .replace(/\s+/g, ' ')
        .trim();

      const keywords = normalizedTitle.split(' ').filter((w) => w.length > 4);
      const duplicates: Dataset[] = [];

      all.forEach((otherDataset, otherIndex) => {
        if (index === otherIndex || processed.has(otherIndex)) return;

        const otherNormalizedTitle = otherDataset.title
          .toLowerCase()
          .replace(/[^a-z0-9\s]/g, '')
          .replace(/\s+/g, ' ')
          .trim();

        const otherKeywords = otherNormalizedTitle.split(' ').filter((w) => w.length > 4);
        const commonKeywords = keywords.filter((k) => otherKeywords.includes(k));

        if (Math.min(keywords.length, otherKeywords.length) > 0) {
          if (commonKeywords.length / Math.min(keywords.length, otherKeywords.length) > 0.6) {
            duplicates.push(otherDataset);
            processed.add(otherIndex);
          }
        }
      });

      groups.push({
        primary: dataset,
        alternates: duplicates,
        hasDuplicates: duplicates.length > 0,
      });

      processed.add(index);
    });

    return groups;
  };

  const calculateStats = (allDatasets: Dataset[], groupedDatasets: DatasetGroup[]) => {
    const bySources: Record<string, number> = {};
    allDatasets.forEach((ds) => {
      bySources[ds.source] = (bySources[ds.source] || 0) + 1;
    });

    setStats({
      total: allDatasets.length,
      unique: groupedDatasets.length,
      bySources,
    });
  };

  const handleSort = (column: typeof sortBy) => {
    if (sortBy === column) {
      setSortOrder(sortOrder === 'asc' ? 'desc' : 'asc');
    } else {
      setSortBy(column);
      setSortOrder('desc');
    }
  };

  const toggleRow = (index: number) => {
    const newExpanded = new Set(expandedRows);
    if (newExpanded.has(index)) newExpanded.delete(index);
    else newExpanded.add(index);
    setExpandedRows(newExpanded);
  };

  const filteredDatasets = datasets.filter((group) => {
    if (sourceFilter !== 'all') {
      const hasSource =
        group.primary.source === sourceFilter ||
        group.alternates.some((alt) => alt.source === sourceFilter);
      if (!hasSource) return false;
    }

    if (modalityFilter !== 'all') {
      const hasModality =
        group.primary.modality.includes(modalityFilter) ||
        group.alternates.some((alt) => alt.modality.includes(modalityFilter));
      if (!hasModality) return false;
    }

    // NOTE: searchQuery is not yet wired to filtering on purpose
    return true;
  });

  const sortedDatasets = [...filteredDatasets].sort((a, b) => {
    let comparison = 0;
    const aData = a.primary;
    const bData = b.primary;

    if (sortBy === 'title') comparison = aData.title.localeCompare(bData.title);
    else if (sortBy === 'id') comparison = aData.id.localeCompare(bData.id);
    else if (sortBy === 'source') comparison = aData.source.localeCompare(bData.source);
    else if (sortBy === 'modality') comparison = aData.modality.localeCompare(bData.modality);
    else if (sortBy === 'citations') comparison = aData.citations - bData.citations;

    return sortOrder === 'asc' ? comparison : -comparison;
  });

  const SortableHeader = ({
    column,
    children,
  }: {
    column: typeof sortBy;
    children: React.ReactNode;
  }) => (
    <th
      className={`px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider cursor-pointer transition-all select-none ${
        darkMode ? 'text-gray-300 hover:bg-white/10' : 'text-gray-700 hover:bg-white/60'
      }`}
      onClick={() => handleSort(column)}
    >
      <div className="flex items-center gap-2">
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
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
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
            <div className="text-2xl font-bold bg-gradient-to-r from-blue-600 to-blue-400 bg-clip-text text-transparent">
              {stats.unique}
            </div>
            <div className={darkMode ? 'text-sm text-gray-300' : 'text-sm text-gray-600'}>Unique Datasets</div>
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
              {stats.bySources['Kaggle'] || 0}
            </div>
            <div className={darkMode ? 'text-sm text-gray-300' : 'text-sm text-gray-600'}>Kaggle</div>
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
          className={`rounded-2xl shadow-xl mb-6 p-4 backdrop-blur-xl ${
            darkMode ? 'bg-white/5 border border-white/10' : 'bg-white/70 border border-white/20'
          }`}
        >
          <div className="flex items-center justify-between flex-wrap gap-4">
            <div className="flex items-center gap-4 flex-wrap">
              <div className="flex items-center gap-2">
                <Filter size={18} className={darkMode ? 'text-gray-300' : 'text-gray-600'} />
                <span className={darkMode ? 'text-sm font-medium text-gray-200' : 'text-sm font-medium text-gray-700'}>
                  Filters:
                </span>
              </div>

              <select
                value={sourceFilter}
                onChange={(e) => setSourceFilter(e.target.value as any)}
                className={`border rounded-xl px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 backdrop-blur-xl transition-all ${
                  darkMode ? 'bg-white/10 border-white/20 text-white' : 'bg-white/80 border-gray-200 text-gray-900'
                }`}
              >
                <option value="all">All Sources</option>
                <option value="DANDI">DANDI</option>
                <option value="Kaggle">Kaggle</option>
                <option value="OpenNeuro">OpenNeuro</option>
                <option value="PhysioNet">PhysioNet</option>
              </select>

              <select
                value={modalityFilter}
                onChange={(e) => setModalityFilter(e.target.value)}
                className={`border rounded-xl px-3 py-1.5 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 backdrop-blur-xl transition-all ${
                  darkMode ? 'bg-white/10 border-white/20 text-white' : 'bg-white/80 border-gray-200 text-gray-900'
                }`}
              >
                <option value="all">All Modalities</option>
                {allModalities.map((modality) => (
                  <option key={modality} value={modality}>
                    {modality}
                  </option>
                ))}
              </select>

              <div className={darkMode ? 'text-sm text-gray-300' : 'text-sm text-gray-600'}>
                Showing{' '}
                <span className={darkMode ? 'font-semibold text-white' : 'font-semibold text-gray-900'}>
                  {sortedDatasets.length}
                </span>{' '}
                unique datasets
              </div>
            </div>

            <button
              onClick={fetchAllDatasets}
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
              onClick={fetchAllDatasets}
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
                          ? 'px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-300'
                          : 'px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-700'
                      }
                    >
                      #
                    </th>
                    <th className="px-4 py-3 w-8"></th>
                    <SortableHeader column="source">Source</SortableHeader>
                    <SortableHeader column="title">Dataset Title</SortableHeader>
                    <SortableHeader column="id">ID</SortableHeader>
                    <SortableHeader column="modality">Modality</SortableHeader>
                    <SortableHeader column="citations">Citations</SortableHeader>
                    <th
                      className={
                        darkMode
                          ? 'px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-300'
                          : 'px-4 py-3 text-left text-xs font-semibold uppercase tracking-wider text-gray-700'
                      }
                    >
                      Links
                    </th>
                  </tr>
                </thead>
                <tbody className={darkMode ? 'divide-y divide-gray-800' : 'divide-y divide-gray-200'}>
                  {sortedDatasets.map((group, index) => {
                    const isExpanded = expandedRows.has(index);
                    return (
                      <React.Fragment
                        key={`${group.primary.source}-${group.primary.id}-${index}`}
                      >
                        <tr className={darkMode ? 'hover:bg-white/5' : 'hover:bg-gray-50'}>
                          <td className="px-4 py-3 text-sm text-gray-800">{index + 1}</td>
                          <td className="px-4 py-3 text-sm">
                            {group.hasDuplicates && (
                              <button
                                onClick={() => toggleRow(index)}
                                className={`inline-flex items-center justify-center h-7 w-7 rounded-full border text-xs transition-colors ${
                                  darkMode
                                    ? 'border-white/20 text-gray-200 hover:bg-white/10'
                                    : 'border-gray-300 text-gray-700 hover:bg-gray-100'
                                }`}
                                aria-label={
                                  isExpanded ? 'Hide alternate sources' : 'Show alternate sources'
                                }
                              >
                                {isExpanded ? <ChevronDown size={16} /> : <ChevronRight size={16} />}
                              </button>
                            )}
                          </td>
                          <td className="px-4 py-3 text-sm">
                            <span
                              className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium ${getSourceBadgeColor(
                                group.primary.source,
                              )}`}
                            >
                              {group.primary.source}
                            </span>
                          </td>
                          <td
                            className="px-4 py-3 text-sm font-medium"
                            style={{ color: darkMode ? '#F9FAFB' : '#111827' }}
                          >
                            {group.primary.title}
                          </td>
                          <td
                            className="px-4 py-3 text-sm"
                            style={{ color: darkMode ? '#E5E7EB' : '#111827' }}
                          >
                            {group.primary.id}
                          </td>
                          <td
                            className="px-4 py-3 text-sm"
                            style={{ color: darkMode ? '#E5E7EB' : '#111827' }}
                          >
                            {group.primary.modality}
                          </td>
                          <td
                            className="px-4 py-3 text-sm font-semibold"
                            style={{ color: darkMode ? '#F9FAFB' : '#111827' }}
                          >
                            {group.primary.citations.toLocaleString()}
                          </td>
                          <td className="px-4 py-3 text-sm">
                            <a
                              href={group.primary.url}
                              target="_blank"
                              rel="noopener noreferrer"
                              className="inline-flex items-center gap-1 text-blue-600 hover:text-blue-500"
                            >
                              Open
                              <ExternalLink size={14} />
                            </a>
                            {group.alternates.length > 0 && (
                              <span className="ml-2 text-xs text-gray-500 dark:text-gray-400">
                                +{group.alternates.length} more source
                                {group.alternates.length > 1 ? 's' : ''}
                              </span>
                            )}
                          </td>
                        </tr>
                        {isExpanded && group.alternates.length > 0 && (
                          <tr className={darkMode ? 'bg-white/5' : 'bg-gray-50'}>
                            <td colSpan={8} className="px-10 py-3">
                              <div className="text-xs font-semibold mb-2 text-gray-500 dark:text-gray-400">
                                Alternate sources for this dataset
                              </div>
                              <div className="space-y-2">
                                {group.alternates.map((alt, altIndex) => (
                                  <div
                                    key={`${alt.source}-${alt.id}-${altIndex}`}
                                    className="flex flex-wrap items-center justify-between gap-2 text-xs"
                                  >
                                    <div className="flex items-center gap-2">
                                      <span
                                        className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium ${getSourceBadgeColor(
                                          alt.source,
                                        )}`}
                                      >
                                        {alt.source}
                                      </span>
                                      <span className="font-medium text-gray-900 dark:text-gray-100">
                                        {alt.title}
                                      </span>
                                    </div>
                                    <div className="flex items-center gap-3">
                                      <span className="text-gray-500 dark:text-gray-400">
                                        {alt.citations.toLocaleString()} citations
                                      </span>
                                      <a
                                        href={alt.url}
                                        target="_blank"
                                        rel="noopener noreferrer"
                                        className="inline-flex items-center gap-1 text-blue-600 hover:text-blue-500"
                                      >
                                        Open
                                        <ExternalLink size={12} />
                                      </a>
                                    </div>
                                  </div>
                                ))}
                              </div>
                            </td>
                          </tr>
                        )}
                      </React.Fragment>
                    );
                  })}
                </tbody>
              </table>
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
          <p className="mt-3 text-xs opacity-75">Developed by the Foresight Institute</p>
        </div>
      </div>
    </div>
  );
}


