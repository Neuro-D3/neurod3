import React, { useState, useEffect, useCallback, useRef } from 'react';
import { createPortal } from 'react-dom';
import { Link } from 'react-router-dom';
import { Skeleton, Spinner } from './Loading';
import { datasetDetailPath, fetchDatasets, fetchDatasetStats } from '../services/api';
import type { Dataset } from '../services/api';
import { PopulationIcon } from './PopulationIcon';
import { formatDay, formatPublishedDate, plural } from '../utils/reuseMetrics';
import { Button, Card, Chip, Eyebrow, INPUT_CLASS, Note, StatGrid, StatTile, Tag, cx } from './ui';

type DatasetSort = 'published' | 'papers' | 'reuse' | 'title' | 'id' | 'source' | 'modality';
// The list opens on the most reused datasets; the URL carries ?sort= only for other orders.
const DEFAULT_SORT: DatasetSort = 'reuse';

const SOURCE_OPTIONS = ['CRCNS', 'DANDI', 'OpenNeuro', 'SPARC'] as const;
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

const INSTITUTION_KEYWORDS = /\b(university|institute|research|laboratory|lab|corporation|inc\.|foundation|centre|center|hospital|medical|school|college|department|dept|national|council|agency|ministry|consortium|group|team|project|program|initiative|society|association|academy|press|pharma|gmbh|ltd|llc)\b/i;

function looksLikeInstitution(authors: string[]): boolean {
  if (authors.length !== 1) return false;
  return INSTITUTION_KEYWORDS.test(authors[0]);
}

// Rotating placeholder examples (search matches title, description, id and authors).
const PROMPT_EXAMPLES = [
  'Search by title, description, dataset id or author',
  'Try "hippocampus", "reaching" or "sleep"',
  'Try an id: 000070, ds000001, pvc-1',
  'Try an author or a lab',
];

export default function NeuroDatasetDiscovery() {
  const [datasets, setDatasets] = useState<Dataset[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [noDatasetsFound, setNoDatasetsFound] = useState<boolean>(false);
  const [sortBy, setSortBy] = useState<DatasetSort>(DEFAULT_SORT);
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [sourceFilter, setSourceFilter] = useState<'all' | 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC'>(
    'all',
  );
  const [selectedModalities, setSelectedModalities] = useState<string[]>([]);
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [stats, setStats] = useState<{ total: number; unique: number; bySources: Record<string, number> }>({
    total: 0,
    unique: 0,
    bySources: {},
  });
  // False until the first stats response (or failure): the tiles pulse instead of reading 0.
  const [statsLoaded, setStatsLoaded] = useState<boolean>(false);
  // True while any stats request is pending; after the first, the tiles dim instead.
  const [statsLoading, setStatsLoading] = useState<boolean>(true);
  const [page, setPage] = useState<number>(1);
  const [pageSize] = useState<number>(25);
  const [totalCount, setTotalCount] = useState<number>(0);
  const [availableSources, setAvailableSources] = useState<Dataset['source'][]>([...SOURCE_OPTIONS]);
  const [availableModalities, setAvailableModalities] = useState<string[]>([]);
  const [modalityDropdownOpen, setModalityDropdownOpen] = useState<boolean>(false);
  const modalityDropdownRef = useRef<HTMLDivElement | null>(null);

  type PapersTooltipState = {
    dataset: Dataset;
    left: number;
    top: number;
    width: number;
    maxHeight: number;
    anchorEl: HTMLElement;
    closing: boolean;
  };
  const [papersTooltip, setPapersTooltip] = useState<PapersTooltipState | null>(null);
  const papersTooltipRef = useRef<HTMLDivElement | null>(null);
  const papersTooltipCloseTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const closePapersTooltip = useCallback(() => {
    setPapersTooltip((prev) => {
      if (!prev) return null;
      if (prev.closing) return prev;
      return { ...prev, closing: true };
    });

    if (papersTooltipCloseTimer.current) clearTimeout(papersTooltipCloseTimer.current);
    papersTooltipCloseTimer.current = setTimeout(() => {
      setPapersTooltip(null);
      papersTooltipCloseTimer.current = null;
    }, 160);
  }, []);
  const openPapersTooltip = useCallback((dataset: Dataset, anchorEl: HTMLElement | null) => {
    if (dataset.source !== 'DANDI' && dataset.source !== 'OpenNeuro') return;
    if (!anchorEl) return;

    if (papersTooltipCloseTimer.current) {
      clearTimeout(papersTooltipCloseTimer.current);
      papersTooltipCloseTimer.current = null;
    }

    // Toggle if clicking the same dataset again.
    if (papersTooltip && papersTooltip.dataset.source === dataset.source && papersTooltip.dataset.id === dataset.id) {
      closePapersTooltip();
      return;
    }

    const rect = anchorEl.getBoundingClientRect();
    const viewportPadding = 8;
    const desiredWidth = 34 * 16; // 34rem
    const width = Math.max(280, Math.min(desiredWidth, window.innerWidth - viewportPadding * 2));
    const centerX = rect.left + rect.width / 2;
    const left = Math.min(
      Math.max(centerX - width / 2, viewportPadding),
      Math.max(viewportPadding, window.innerWidth - width - viewportPadding),
    );
    const top = Math.min(rect.bottom + 8, window.innerHeight - viewportPadding);
    const maxHeight = Math.max(160, window.innerHeight - top - viewportPadding);

    setPapersTooltip({ dataset, left, top, width, maxHeight, anchorEl, closing: false });
  }, [papersTooltip, closePapersTooltip]);

  useEffect(() => {
    if (!papersTooltip) return;

    const onMouseDown = (e: MouseEvent) => {
      const target = e.target as Node | null;
      if (!target) return;

      if (papersTooltipRef.current && papersTooltipRef.current.contains(target)) return;
      if (papersTooltip.anchorEl && papersTooltip.anchorEl.contains(target)) return;

      closePapersTooltip();
    };

    const onKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closePapersTooltip();
    };

    // Capture so it works even if inner handlers stop propagation.
    document.addEventListener('mousedown', onMouseDown, true);
    document.addEventListener('keydown', onKeyDown);
    return () => {
      document.removeEventListener('mousedown', onMouseDown, true);
      document.removeEventListener('keydown', onKeyDown);
    };
  }, [papersTooltip, closePapersTooltip]);

  useEffect(() => {
    return () => {
      if (papersTooltipCloseTimer.current) clearTimeout(papersTooltipCloseTimer.current);
    };
  }, []);

  useEffect(() => {
    if (!papersTooltip) return;
    const close = () => closePapersTooltip();
    // Capture scroll events from nested containers (table wrapper, etc.)
    window.addEventListener('scroll', close, true);
    window.addEventListener('resize', close);
    return () => {
      window.removeEventListener('scroll', close, true);
      window.removeEventListener('resize', close);
    };
  }, [papersTooltip, closePapersTooltip]);

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

    const searchParam = params.get('search')?.trim();
    if (searchParam) {
      setSearchQuery(searchParam);
    }

    const pageParam = params.get('page');
    if (pageParam) {
      const p = parseInt(pageParam, 10);
      if (p > 0) setPage(p);
    }

    const sortParam = params.get('sort')?.trim();
    if (sortParam && ['published', 'papers', 'reuse', 'title', 'id', 'source', 'modality'].includes(sortParam)) {
      setSortBy(sortParam as typeof sortBy);
    }

    const orderParam = params.get('order')?.trim().toLowerCase();
    if (orderParam && (orderParam === 'asc' || orderParam === 'desc')) {
      setSortOrder(orderParam);
    }
  }, []);

  useEffect(() => {
    const interval = setInterval(() => {
      setPlaceholderIndex((prev) => (prev + 1) % PROMPT_EXAMPLES.length);
    }, 4000);
    return () => clearInterval(interval);
  }, []);

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, selectedModalities, searchQuery]);

  // Keep URL in sync with active filters (shareable links).
  useEffect(() => {
    if (typeof window === 'undefined') return;

    const url = new URL(window.location.href);
    const params = url.searchParams;

    if (sourceFilter === 'all') params.delete('source');
    else params.set('source', sourceFilter);

    if (selectedModalities.length === 0) params.delete('modality');
    else params.set('modality', selectedModalities.join(','));

    if (!searchQuery.trim()) params.delete('search');
    else params.set('search', searchQuery.trim());

    if (page <= 1) params.delete('page');
    else params.set('page', String(page));

    if (sortBy === DEFAULT_SORT) params.delete('sort');
    else params.set('sort', sortBy);

    if (sortOrder === 'desc') params.delete('order');
    else params.set('order', sortOrder);

    // Avoid pushing a new history entry for each change.
    window.history.replaceState(null, '', `${url.pathname}${params.toString() ? `?${params.toString()}` : ''}`);
  }, [sourceFilter, selectedModalities, searchQuery, page, sortBy, sortOrder]);

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

  const fetchAllDatasets = useCallback(async (isCancelled?: () => boolean) => {
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
        search: searchQuery.trim() || undefined,
        sort_by: sortBy,
        sort_order: sortOrder,
        limit: pageSize,
        offset,
      });
      // Drop the result if this effect run was superseded — otherwise a slower
      // earlier fetch can clobber a faster later one (manifests as "Showing 0
      // of <unfiltered total>" on back-navigation to ?source=CRCNS).
      if (isCancelled?.()) return;
      const allDatasets: Dataset[] = response.datasets;

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
      if (isCancelled?.()) return;
      const errorMessage = err instanceof Error ? err.message : 'Failed to connect to API';
      console.error('Error fetching datasets from API:', err);
      setError(`API connection error: ${errorMessage}. Please ensure the backend service is running.`);
      setNoDatasetsFound(false);
      setTotalCount(0);
      setLoading(false);
    }
  }, [page, pageSize, sourceFilter, selectedModalities, searchQuery, sortBy, sortOrder]);

  useEffect(() => {
    let cancelled = false;
    fetchAllDatasets(() => cancelled);
    return () => {
      cancelled = true;
    };
  }, [fetchAllDatasets]);

  const fetchStats = useCallback(async (isCancelled?: () => boolean) => {
    setStatsLoading(true);
    try {
      const sourceParam = sourceFilter !== 'all' ? sourceFilter : undefined;
      const modalitiesParam = selectedModalities.length ? selectedModalities : undefined;

      const response = await fetchDatasetStats({
        source: sourceParam,
        modalities: modalitiesParam,
        search: searchQuery.trim() || undefined,
      });
      if (isCancelled?.()) return;

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
      if (isCancelled?.()) return;
      console.error('Error fetching dataset stats from API:', err);
    } finally {
      if (!isCancelled?.()) {
        setStatsLoaded(true);
        setStatsLoading(false);
      }
    }
  }, [sourceFilter, selectedModalities, searchQuery]);

  useEffect(() => {
    let cancelled = false;
    fetchStats(() => cancelled);
    return () => {
      cancelled = true;
    };
  }, [fetchStats]);

  // If current selections become unavailable, reset to All.
  useEffect(() => {
    if (sourceFilter !== 'all' && !availableSources.includes(sourceFilter)) {
      setSourceFilter('all');
    }
  }, [availableSources, sourceFilter]);

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

    if (searchQuery.trim()) {
      const q = searchQuery.trim().toLowerCase();
      const title = (ds.title || '').toLowerCase();
      const description = (typeof ds.description === 'string' ? ds.description : '').toLowerCase();
      const id = (ds.id || '').toLowerCase();
      const authors = (ds.authors || []).join(' ').toLowerCase();
      if (!title.includes(q) && !description.includes(q) && !id.includes(q) && !authors.includes(q)) return false;
    }

    return true;
  });

  const isSelectedModality = (token: string) => {
    const norm = normalizeModalityForCompare(token);
    return selectedModalities.some((m) => normalizeModalityForCompare(m) === norm);
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

  // IMPORTANT: sorting is done server-side so pagination is correct.
  const sortedDatasets = filteredDatasets;
  // Nothing on screen yet: placeholder rows instead of a spinner.
  const firstLoad = loading && datasets.length === 0;

  const totalPages = Math.max(1, Math.ceil(totalCount / pageSize));

  useEffect(() => {
    if (page > totalPages) {
      setPage(totalPages);
    }
  }, [page, totalPages]);

  const handleRefresh = () => {
    fetchAllDatasets();
    fetchStats();
  };

  const sourceChoices: Array<'all' | Dataset['source']> = ['all', ...availableSources];

  return (
    <div className="min-h-screen bg-canvas">
      {/* Hero: continues the masthead's navy with the title and the search field. */}
      <section className="bg-navy text-white">
        <div className="mx-auto flex max-w-[1440px] flex-col gap-4 px-4 pb-7 pt-5 sm:px-6 lg:px-8">
          <div className="flex flex-col gap-2">
            <span className="font-mono text-xs uppercase tracking-[0.08em] text-navy-muted">NeuroD3 · Dataset Reuse Hub</span>
            <h1 className="m-0 text-[28px] font-bold leading-[1.1] tracking-[-0.005em] sm:text-[36px]">
              Neuroscience datasets and the papers that reuse them
            </h1>
            <p className="m-0 max-w-3xl text-[15px] leading-[1.45] text-navy-sub sm:text-[17px]">
              Datasets from DANDI, OpenNeuro, CRCNS and SPARC, with every citing paper read and labelled as reuse or a
              mention.
            </p>
          </div>
          <label className="relative block max-w-3xl">
            <span className="sr-only">Search datasets</span>
            <input
              type="search"
              placeholder={searchQuery ? '' : PROMPT_EXAMPLES[placeholderIndex]}
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              className="h-12 w-full rounded-[5px] border border-navy-tag bg-surface pl-11 pr-4 text-[15px] text-ink placeholder:text-muted focus:border-focus focus:outline-none"
            />
            <SearchIcon className="pointer-events-none absolute left-4 top-1/2 -translate-y-1/2 text-muted" />
          </label>
        </div>
      </section>

      {/* Filter band */}
      <div className="sticky top-[49px] z-30 border-b border-line bg-surface">
        <div className="mx-auto flex max-w-[1440px] flex-wrap items-center gap-x-5 gap-y-2.5 px-4 py-2.5 sm:px-6 lg:px-8">
          <div className="flex flex-wrap items-center gap-1.5">
            <Eyebrow className="mr-0.5">Source</Eyebrow>
            {sourceChoices.map((s) => (
              <Button
                key={s}
                size="sm"
                variant={sourceFilter === s ? 'primary' : 'ghost'}
                aria-pressed={sourceFilter === s}
                onClick={() => setSourceFilter(s)}
              >
                {s === 'all' ? 'All' : s}
              </Button>
            ))}
          </div>

          <div className="flex items-center gap-1.5" ref={modalityDropdownRef}>
            <Eyebrow className="mr-0.5">Modality</Eyebrow>
            <div className="relative">
              <button
                type="button"
                onClick={() => setModalityDropdownOpen((v) => !v)}
                aria-expanded={modalityDropdownOpen}
                aria-haspopup="listbox"
                className={cx(INPUT_CLASS, 'flex h-8 w-44 items-center justify-between gap-2 text-left text-[13px]')}
                title={
                  selectedModalities.length
                    ? selectedModalities.map((m) => formatModalityToken(m)).join(', ')
                    : 'All modalities'
                }
              >
                <span className="truncate">
                  {selectedModalities.length === 0
                    ? 'All modalities'
                    : selectedModalities.length === 1
                      ? formatModalityToken(selectedModalities[0])
                      : `${selectedModalities.length} selected`}
                </span>
                <span aria-hidden="true" className="text-muted">
                  ▾
                </span>
              </button>

              {modalityDropdownOpen && (
                <div className="absolute left-0 z-50 mt-1.5 max-h-72 w-64 overflow-auto rounded-md border border-line bg-surface p-1.5 shadow-pop">
                  <button
                    type="button"
                    onClick={() => setSelectedModalities([])}
                    className="w-full rounded px-2 py-1.5 text-left text-sm font-bold text-ink hover:bg-panel"
                  >
                    All modalities
                  </button>
                  <div className="my-1.5 h-px bg-line-2" />

                  {availableModalities.length === 0 ? (
                    <div className="px-2 py-1.5 text-sm text-muted">No modalities available</div>
                  ) : (
                    availableModalities.map((modality) => {
                      const checked = selectedModalities.includes(modality);
                      return (
                        <label
                          key={modality}
                          className="flex cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-sm text-ink hover:bg-panel"
                        >
                          <input
                            type="checkbox"
                            checked={checked}
                            className="accent-navy"
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
          </div>

          <div className="flex items-center gap-1.5">
            <Eyebrow className="mr-0.5">Sort</Eyebrow>
            <select
              value={`${sortBy}-${sortOrder}`}
              onChange={(e) => {
                const [col, ord] = e.target.value.split('-') as [typeof sortBy, 'asc' | 'desc'];
                setSortBy(col);
                setSortOrder(ord);
                setPage(1);
              }}
              aria-label="Sort datasets"
              className={cx(INPUT_CLASS, 'h-8 text-[13px]')}
            >
              <option value="reuse-desc">Most reused</option>
              <option value="published-desc">Newest first</option>
              <option value="published-asc">Oldest first</option>
              <option value="papers-desc">Most papers</option>
              <option value="papers-asc">Fewest papers</option>
              <option value="title-asc">Title A–Z</option>
              <option value="title-desc">Title Z–A</option>
            </select>
          </div>

          <div className="ml-auto flex items-center gap-3">
            <span className="flex items-center gap-2 text-[13px] text-muted" aria-live="polite">
              {loading ? <Spinner /> : null}
              <span>
                {firstLoad ? (
                  'Loading datasets…'
                ) : (
                  <>
                    Showing <span className="font-bold text-ink">{sortedDatasets.length}</span> of{' '}
                    <span className="font-bold text-ink">{totalCount.toLocaleString()}</span> datasets
                  </>
                )}
              </span>
            </span>
            <Button size="sm" variant="ghost" onClick={handleRefresh} disabled={loading}>
              <span aria-hidden="true" className={loading ? 'inline-block animate-spin' : ''}>
                ⟳
              </span>
              Refresh
            </Button>
          </div>
        </div>
      </div>

      <div className="mx-auto flex max-w-[1440px] flex-col gap-6 px-4 py-6 sm:px-6 lg:px-8">
        {/* Archive totals */}
        <div className={cx('transition-opacity', statsLoading && statsLoaded ? 'opacity-50' : '')}>
          <StatGrid columns={5} className="bg-surface shadow-card max-[640px]:!grid-cols-2 max-[640px]:[&>*:nth-child(3)]:border-l-0 max-[640px]:[&>*:nth-child(5)]:border-l-0 max-[640px]:[&>*:nth-child(n+3)]:border-t">
            <StatTile first label="Datasets" value={stats.total.toLocaleString()} valueClassName="text-link" loading={!statsLoaded}>
              <span className="text-xs text-muted">{sourceFilter === 'all' && !selectedModalities.length && !searchQuery.trim() ? 'across four archives' : 'matching the filters'}</span>
            </StatTile>
            {SOURCE_OPTIONS.map((s) => (
              <StatTile key={s} label={s} value={(stats.bySources[s] || 0).toLocaleString()} loading={!statsLoaded}>
                <span className="text-xs text-muted">datasets</span>
              </StatTile>
            ))}
          </StatGrid>
        </div>

        {/* Dataset list */}
        {error ? (
          <Card className="items-center py-10 text-center">
            <h2 className="m-0 text-lg font-bold text-danger-ink">Connection error</h2>
            <p className="m-0 max-w-lg text-sm text-ink-2">{error}</p>
            <Button variant="primary" onClick={handleRefresh}>
              Retry connection
            </Button>
          </Card>
        ) : noDatasetsFound ? (
          <Card className="items-center py-10 text-center">
            <h2 className="m-0 text-lg font-bold text-ink">No datasets found</h2>
            <p className="m-0 max-w-lg text-sm text-ink-2">
              No datasets match the current filters, or the database may be empty.
            </p>
            <Button variant="primary" onClick={handleRefresh}>
              Retry
            </Button>
          </Card>
        ) : (
          <section aria-label="Datasets" className="overflow-hidden rounded-md border border-line bg-surface shadow-card">
            <div
              aria-busy={loading}
              className={cx('flex flex-col transition-opacity', loading && !firstLoad ? 'opacity-50' : '')}
            >
              {firstLoad ? (
                <DatasetFeedSkeleton count={6} />
              ) : (
                sortedDatasets.map((ds, index) => {
                  const desc = (ds.description || '').trim();
                  const shortDesc = desc.length > 220 ? desc.slice(0, 220).replace(/\s+\S*$/, '') + '…' : desc;
                  const parts = (ds.modality || '').split(/[;,]/).map((p) => p.trim()).filter(Boolean);
                  const primaryPapers = ds.papers ?? 0;
                  const reusePapers = ds.reuse_count ?? 0;
                  const paperCount = primaryPapers + reusePapers;
                  const canOpenPapers = ds.source === 'DANDI' || ds.source === 'OpenNeuro';

                  return (
                    <article
                      key={`${ds.source}-${ds.id}-${index}`}
                      className="grid gap-x-6 gap-y-3 border-t border-line-3 px-5 py-4 first:border-t-0 hover:bg-panel-head sm:grid-cols-[minmax(0,1fr)_auto]"
                    >
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-baseline gap-x-2.5 gap-y-1">
                          <Link
                            to={datasetDetailPath(ds.source, ds.id)}
                            className="text-[15.5px] font-bold leading-snug text-ink hover:text-link"
                          >
                            {ds.title}
                          </Link>
                          <span className="font-mono text-xs text-muted">
                            {ds.source}:{ds.id}
                          </span>
                        </div>

                        {shortDesc && (
                          <p className="m-0 mt-1 line-clamp-2 text-[13.5px] leading-[1.5] text-ink-3">{shortDesc}</p>
                        )}

                        <div className="mt-2.5 flex flex-wrap items-center gap-x-2.5 gap-y-1.5 text-xs text-muted">
                          <Chip
                            active={sourceFilter === ds.source}
                            onClick={() => setSourceFilter((prev) => (prev === ds.source ? 'all' : ds.source))}
                            title="Click to toggle filter"
                          >
                            {ds.source}
                          </Chip>
                          <span className="tabular-nums">
                            Released {formatPublishedDate(ds.created_at, ds.created_at_precision) ?? '—'}
                          </span>
                          {ds.authors && ds.authors.length > 0 && (
                            <span>
                              {looksLikeInstitution(ds.authors)
                                ? ds.authors[0]
                                : plural(ds.authors.length, 'author', 'authors')}
                            </span>
                          )}
                          {parts.length > 0 && (
                            <span className="flex flex-wrap items-center gap-1">
                              {parts.slice(0, 3).map((m) => (
                                <Chip
                                  key={m}
                                  active={isSelectedModality(m)}
                                  onClick={() => toggleSelectedModality(m)}
                                  title="Click to toggle filter"
                                >
                                  {formatModalityToken(m)}
                                </Chip>
                              ))}
                              {parts.length > 3 && <span className="px-1">+{parts.length - 3}</span>}
                            </span>
                          )}
                        </div>
                      </div>

                      <div className="flex flex-row flex-wrap items-center gap-x-4 gap-y-2 sm:w-40 sm:flex-col sm:items-end sm:gap-2">
                        {reusePapers > 0 ? (
                          <Tag tone="reuse" title="Citing papers whose full text shows they used the data">
                            {plural(reusePapers, 'reuse', 'reuses')}
                          </Tag>
                        ) : (
                          <Tag tone="neutral" title="No citing paper has been found to use the data yet">
                            no reuse yet
                          </Tag>
                        )}
                        <button
                          type="button"
                          className={cx(
                            'text-xs tabular-nums text-ink-2',
                            canOpenPapers ? 'cursor-pointer hover:text-link hover:underline' : 'cursor-default',
                          )}
                          onClick={(e) => {
                            e.preventDefault();
                            openPapersTooltip(ds, e.currentTarget);
                          }}
                          disabled={!canOpenPapers}
                          aria-label={`${paperCount} associated papers`}
                        >
                          {plural(paperCount, 'paper', 'papers')}
                        </button>
                        {ds.num_subjects != null && ds.num_subjects > 0 && (
                          <span className="inline-flex items-center gap-1 text-xs tabular-nums text-muted">
                            <PopulationIcon size={13} />
                            {ds.num_subjects.toLocaleString()} {ds.source === 'OpenNeuro' ? 'participants' : 'subjects'}
                          </span>
                        )}
                        {ds.updated_at && (
                          <span className="text-xs tabular-nums text-muted">
                            Updated {formatDay(ds.updated_at)}
                          </span>
                        )}
                      </div>
                    </article>
                  );
                })
              )}
            </div>

            {/* Pagination */}
            <div className="flex items-center justify-between gap-3 border-t border-line-2 bg-panel-head px-5 py-3 text-sm text-ink-2">
              <span>
                Page <span className="font-bold text-ink">{page}</span> of{' '}
                <span className="font-bold text-ink">{totalPages}</span>
              </span>
              <div className="flex gap-2">
                <Button size="sm" variant="ghost" onClick={() => setPage((prev) => Math.max(1, prev - 1))} disabled={page <= 1 || loading}>
                  Previous
                </Button>
                <Button size="sm" variant="ghost" onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))} disabled={page >= totalPages || loading}>
                  Next
                </Button>
              </div>
            </div>
          </section>
        )}

        <Note>
          Reuse counts are citing papers whose full text shows they used the data; a dataset's own papers are counted
          separately. Datasets with similar titles may be the same data on different archives.
        </Note>
      </div>

      {papersTooltip &&
        createPortal(
          <div
            id={`papers-popover-${papersTooltip.dataset.source}-${papersTooltip.dataset.id}`}
            ref={papersTooltipRef}
            className={cx(
              'fixed z-[9999] rounded-md border border-line bg-surface p-3.5 text-left text-ink shadow-pop transition-opacity transition-transform duration-150',
              papersTooltip.closing
                ? 'pointer-events-none -translate-y-1 scale-[0.98] opacity-0'
                : 'pointer-events-auto translate-y-0 scale-100 opacity-100',
            )}
            style={{
              left: papersTooltip.left,
              top: papersTooltip.top,
              width: papersTooltip.width,
              maxHeight: papersTooltip.maxHeight,
            }}
            role="dialog"
            aria-label="Associated papers"
          >
            <Eyebrow className="mb-2 block">Associated papers</Eyebrow>

            {papersTooltip.dataset.papers == null ? (
              <div className="text-sm text-ink-2">Papers not computed yet.</div>
            ) : Array.isArray(papersTooltip.dataset.paper_dois) && papersTooltip.dataset.paper_dois.length > 0 ? (
              (() => {
                const maxShow = 10;
                const dois = papersTooltip.dataset.paper_dois ?? [];
                const titles = Array.isArray(papersTooltip.dataset.paper_titles)
                  ? papersTooltip.dataset.paper_titles
                  : [];
                const visibleDois = dois.slice(0, maxShow);
                const remaining = dois.length - visibleDois.length;
                const canonicalizeDoiForLink = (doiRaw: string) => {
                  const doi = String(doiRaw || '').trim();
                  if (!doi) return doi;
                  // bioRxiv / medRxiv appear with `vN` (and sometimes `.abstract`) in free text; doi.org often 404s on those.
                  if (/^10\.1101\//i.test(doi)) {
                    return doi
                      .replace(/(v\d+)(\.(abstract|full|pdf))?$/i, '')
                      .replace(/\.(abstract|full|pdf)$/i, '');
                  }
                  return doi;
                };
                const doiToHref = (doiRaw: string) => {
                  const doi = canonicalizeDoiForLink(doiRaw);
                  return `https://doi.org/${encodeURIComponent(doi).replace(/%2F/gi, '/')}`;
                };
                return (
                  <div className="space-y-1 overflow-auto" style={{ maxHeight: papersTooltip.maxHeight - 40 }}>
                    <div className="grid grid-cols-1 gap-1.5 text-[13.5px]">
                      {visibleDois.map((doi, i) => {
                        const title = titles[i];
                        const doiCanon = canonicalizeDoiForLink(doi);
                        const label = title && String(title).trim().length > 0 ? title : doiCanon;
                        return (
                          <a
                            key={`${papersTooltip.dataset.source}-${papersTooltip.dataset.id}-paper-${i}`}
                            href={doiToHref(doi)}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="pointer-events-auto leading-snug"
                            title={doiCanon}
                          >
                            {label}
                          </a>
                        );
                      })}
                    </div>
                    {remaining > 0 && <div className="text-xs text-muted">+{remaining} more</div>}
                  </div>
                );
              })()
            ) : (
              <div className="text-sm text-ink-2">No paper titles found yet.</div>
            )}
          </div>,
          document.body,
        )}
    </div>
  );
}

function SearchIcon({ className }: { className?: string }) {
  return (
    <svg className={className} width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <circle cx="11" cy="11" r="7" />
      <path d="m20 20-3.5-3.5" />
    </svg>
  );
}

/** Placeholder rows shaped like the dataset list, shown until the first page arrives. */
function DatasetFeedSkeleton({ count }: { count: number }) {
  return (
    <div aria-busy="true" aria-label="Loading datasets">
      {Array.from({ length: count }, (_, i) => (
        <div key={i} className="grid gap-x-6 gap-y-3 border-t border-line-3 px-5 py-4 first:border-t-0 sm:grid-cols-[minmax(0,1fr)_auto]">
          <div className="min-w-0 space-y-2.5">
            <Skeleton className="h-4 w-2/3 rounded" />
            <Skeleton className="h-3 w-full rounded" />
            <Skeleton className="h-3 w-5/6 rounded" />
            <Skeleton className="h-5 w-1/3 rounded" />
          </div>
          <div className="flex gap-2 sm:w-40 sm:flex-col sm:items-end">
            <Skeleton className="h-[22px] w-20 rounded" />
            <Skeleton className="h-3 w-16 rounded" />
          </div>
        </div>
      ))}
    </div>
  );
}
