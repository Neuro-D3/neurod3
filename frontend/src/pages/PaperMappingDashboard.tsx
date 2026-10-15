import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import { Skeleton, Spinner } from '../components/Loading';
import { Button, Eyebrow, INPUT_CLASS } from '../components/ui';
import { formatDay } from '../utils/reuseMetrics';
import {
  datasetDetailPath,
  fetchPaperMappingCitations,
  fetchPaperMappingDatasetDetail,
  fetchPaperMappingDatasets,
  fetchPaperMappingSummary,
  PaperMappingCitation,
  PaperMappingDatasetDetail,
  PaperMappingDatasetRow,
  PaperMappingSourceSummary,
  PaperMappingSummary,
} from '../services/api';
import {
  ClassificationProgress,
  classificationProgress,
  confidenceShort,
  modalityLabel,
  primaryQuote,
  reuseTypeLabel,
  reusedModalities,
  statusBadgeClass,
  statusFillClass,
  statusLabel,
} from '../utils/classification';

type SourceFilter = 'all' | 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';

/** Matches `animate-panel-out` in tailwind.config.js; the panel unmounts after it. */
const PANEL_CLOSE_MS = 220;
type SortKey =
  | 'mapped_papers'
  | 'citation_edges'
  | 'contexts_extracted'
  | 'latest_primary_publication_date'
  | 'latest_citing_publication_date'
  | 'title'
  | 'id'
  | 'source';

function formatDate(value?: string | null): string {
  return formatDay(value) ?? '—';
}

function formatNumber(value?: number | null): string {
  if (typeof value !== 'number') return '0';
  return value.toLocaleString();
}

/**
 * For counts that are null until the DAGs have written dataset_status: an
 * unknown count shows as a dash, not as zero.
 */
function formatCount(value?: number | null): string {
  if (typeof value !== 'number') return '—';
  return value.toLocaleString();
}

function truncate(text?: string | null, max = 160): string {
  if (!text) return '';
  if (text.length <= max) return text;
  return `${text.slice(0, max - 1)}…`;
}

const PAGE_SIZE = 20;

export default function PaperMappingDashboard() {
  const [sourceFilter, setSourceFilter] = useState<SourceFilter>('all');
  const [search, setSearch] = useState('');
  const [classificationBucketFilter, setClassificationBucketFilter] = useState<string | null>(null);
  const [sortBy, setSortBy] = useState<SortKey>('mapped_papers');
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc'>('desc');
  const [page, setPage] = useState(1);

  const [summary, setSummary] = useState<PaperMappingSummary | null>(null);
  const [datasets, setDatasets] = useState<PaperMappingDatasetRow[]>([]);
  const [datasetCount, setDatasetCount] = useState(0);
  const [selectedDataset, setSelectedDataset] = useState<PaperMappingDatasetRow | null>(null);
  // The drill-down panel slides in when it mounts (CSS animation) and slides
  // out on close; `selectedDataset` stays set until the slide-out finishes.
  const [panelClosing, setPanelClosing] = useState(false);
  const closeTimer = useRef<number | null>(null);
  const panelRef = useRef<HTMLElement>(null);
  const [datasetDetail, setDatasetDetail] = useState<PaperMappingDatasetDetail | null>(null);
  const [citationsPreview, setCitationsPreview] = useState<PaperMappingCitation[]>([]);
  const [citationsTotal, setCitationsTotal] = useState(0);

  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sourceParam = sourceFilter === 'all' ? undefined : sourceFilter;
  const totalPages = Math.max(1, Math.ceil(datasetCount / PAGE_SIZE));

  useEffect(() => {
    setPage(1);
  }, [sourceFilter, search, classificationBucketFilter]);

  const openPanel = useCallback((dataset: PaperMappingDatasetRow) => {
    if (closeTimer.current !== null) {
      window.clearTimeout(closeTimer.current);
      closeTimer.current = null;
    }
    setPanelClosing(false);
    setSelectedDataset(dataset);
  }, []);

  const closePanel = useCallback(() => {
    setPanelClosing(true);
    if (closeTimer.current !== null) window.clearTimeout(closeTimer.current);
    closeTimer.current = window.setTimeout(() => {
      setSelectedDataset(null);
      setPanelClosing(false);
      closeTimer.current = null;
    }, PANEL_CLOSE_MS);
  }, []);

  useEffect(() => () => {
    if (closeTimer.current !== null) window.clearTimeout(closeTimer.current);
  }, []);

  useEffect(() => {
    closePanel();
  }, [classificationBucketFilter, closePanel]);

  useEffect(() => {
    let cancelled = false;
    async function load() {
      setLoading(true);
      setError(null);
      try {
        const [summaryResp, datasetsResp] = await Promise.all([
          fetchPaperMappingSummary({ source: sourceParam }),
          fetchPaperMappingDatasets({
            source: sourceParam,
            search: search || undefined,
            classification_bucket: classificationBucketFilter || undefined,
            sort_by: sortBy,
            sort_order: sortOrder,
            limit: PAGE_SIZE,
            offset: (page - 1) * PAGE_SIZE,
          }),
        ]);
        if (cancelled) return;
        setSummary(summaryResp);
        setDatasets(datasetsResp.datasets);
        setDatasetCount(datasetsResp.count);
      } catch (err: any) {
        if (cancelled) return;
        setError(err?.message || String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }
    load();
    return () => {
      cancelled = true;
    };
  }, [page, search, sortBy, sortOrder, sourceParam, classificationBucketFilter]);

  useEffect(() => {
    let cancelled = false;
    async function loadDetail() {
      if (!selectedDataset) {
        setDatasetDetail(null);
        setCitationsPreview([]);
        return;
      }
      setDetailLoading(true);
      try {
        const [detailResp, citationsResp] = await Promise.all([
          fetchPaperMappingDatasetDetail(selectedDataset.source, selectedDataset.dataset_id),
          fetchPaperMappingCitations({
            source: selectedDataset.source,
            dataset_id: selectedDataset.dataset_id,
            limit: 25,
            offset: 0,
          }),
        ]);
        if (cancelled) return;
        setDatasetDetail(detailResp);
        setCitationsPreview(citationsResp.citations);
        setCitationsTotal(citationsResp.count ?? citationsResp.citations.length);
      } catch (err: any) {
        if (cancelled) return;
        setError(err?.message || String(err));
      } finally {
        if (!cancelled) setDetailLoading(false);
      }
    }
    loadDetail();
    return () => {
      cancelled = true;
    };
  }, [selectedDataset]);

  useEffect(() => {
    if (!selectedDataset || panelClosing) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closePanel();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [selectedDataset, panelClosing, closePanel]);

  // A click outside the drill-down slides it out. A click on another dataset
  // row is left to the row, which switches the panel to that dataset.
  useEffect(() => {
    if (!selectedDataset || panelClosing) return;
    const onPointerDown = (e: PointerEvent) => {
      if (e.button !== 0) return;
      const target = e.target;
      if (!(target instanceof Element)) return;
      if (panelRef.current?.contains(target) || target.closest('[data-opens-panel]')) return;
      closePanel();
    };
    document.addEventListener('pointerdown', onPointerDown);
    return () => document.removeEventListener('pointerdown', onPointerDown);
  }, [selectedDataset, panelClosing, closePanel]);

  const classificationBreakdown = useMemo(() => {
    if (!summary) return [];
    return Object.entries(summary.by_classification).sort((a, b) => b[1] - a[1]);
  }, [summary]);

  const toggleSort = (key: SortKey) => {
    if (sortBy === key) {
      setSortOrder((prev) => (prev === 'asc' ? 'desc' : 'asc'));
    } else {
      setSortBy(key);
      setSortOrder(key === 'title' || key === 'id' || key === 'source' ? 'asc' : 'desc');
    }
  };

  // Nothing fetched yet: placeholders. Later loads (filter, sort, page) keep the
  // previous data on screen, dimmed, so the layout doesn't jump.
  const firstLoad = loading && !summary;
  // Applied to everything that shows fetched data, so stale numbers are visibly stale.
  const reloadDim = `transition-opacity ${loading && !firstLoad ? 'opacity-50' : ''}`;

  return (
    <div className="min-h-screen bg-canvas text-ink">
      {/* Page header: continues the masthead's navy. */}
      <section className="bg-navy text-white">
        <div className="mx-auto flex max-w-[1440px] flex-col gap-2 px-4 pb-6 pt-5 sm:px-6 lg:px-8">
          <span className="font-mono text-xs uppercase tracking-[0.08em] text-navy-muted">NeuroD3 · Internal</span>
          <h1 className="m-0 text-[28px] font-bold leading-[1.1] tracking-[-0.005em] sm:text-[36px]">Paper Mapping Dashboard</h1>
          <p className="m-0 max-w-3xl text-[15px] leading-[1.45] text-navy-sub">
            Mapped primary papers, citation enrichment coverage, and whole-paper reuse classifications (Reuse / Mention /
            Neither, with verified quotes) across DANDI, OpenNeuro, CRCNS and SPARC.
          </p>
        </div>
      </section>

      {/* Filter band */}
      <div className="sticky top-[49px] z-30 border-b border-line bg-surface">
        <div className="mx-auto flex max-w-[1440px] flex-wrap items-center gap-x-5 gap-y-2.5 px-4 py-2.5 sm:px-6 lg:px-8">
          <div className="flex flex-wrap items-center gap-1.5">
            <Eyebrow className="mr-0.5">Source</Eyebrow>
            {(['all', 'DANDI', 'OpenNeuro', 'CRCNS', 'SPARC'] as SourceFilter[]).map((option) => (
              <Button
                key={option}
                size="sm"
                variant={sourceFilter === option ? 'primary' : 'ghost'}
                aria-pressed={sourceFilter === option}
                onClick={() => setSourceFilter(option)}
              >
                {option === 'all' ? 'All' : option}
              </Button>
            ))}
          </div>
          <label className="relative ml-auto block w-full max-w-md">
            <span className="sr-only">Search datasets</span>
            <input
              className={`${INPUT_CLASS} h-8 w-full text-[13px]`}
              placeholder="Search dataset id, title, or description"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </label>
        </div>
      </div>

      <div className="mx-auto max-w-[1440px] px-4 py-6 sm:px-6 lg:px-8">

        {error ? (
          <div className="mb-6 rounded-md border border-danger bg-danger-tint px-4 py-3 text-sm text-danger-ink">{error}</div>
        ) : null}

        {/* All sources: the five totals. One archive: its funnel card takes the
            cards' place, since the totals would just repeat the funnel's numbers. */}
        {sourceFilter === 'all' ? (
        <div className={`grid gap-4 md:grid-cols-3 lg:grid-cols-5 ${reloadDim}`}>
          <SummaryCard
            loading={firstLoad}
            title="Ingested Datasets"
            value={formatCount(summary?.summary.ingested_datasets)}
            hint={
              typeof summary?.summary.ingested_total === 'number'
                ? `${formatNumber(summary.summary.ingested_total)} ingested, junk removed`
                : 'Run an ingestion DAG to compute'
            }
          />
          <SummaryCard
            loading={firstLoad}
            title="Junk Excluded"
            value={formatCount(summary?.summary.junk_datasets)}
            hint="Test, placeholder and empty uploads; hidden from the dataset list"
          />
          <SummaryCard
            loading={firstLoad}
            title="Datasets With Mapped Papers"
            value={formatNumber(summary?.summary.datasets_with_mapped_papers)}
            hint={
              typeof summary?.summary.ingested_datasets === 'number' && summary.summary.ingested_datasets > 0
                ? `${Math.round((100 * summary.summary.datasets_with_mapped_papers) / summary.summary.ingested_datasets)}% of ingested`
                : undefined
            }
          />
          <SummaryCard title="Distinct Primary Papers" loading={firstLoad} value={formatNumber(summary?.summary.distinct_mapped_primary_papers)} />
          <SummaryCard title="Citation Edges" loading={firstLoad} value={formatNumber(summary?.summary.citation_edges)} />
        </div>
        ) : (
          <section aria-labelledby="source-breakdown-heading">
            <h2 id="source-breakdown-heading" className="sr-only">
              Source Breakdown
            </h2>
            {/* Only the filtered archive: right after a click `summary` is still the
                previous response (all four archives) until the new fetch lands. */}
            {(summary?.by_source || []).filter((entry) => entry.source === sourceFilter).map((entry) => (
              <div key={entry.source} className={`rounded-md border border-line bg-surface p-4 shadow-card ${reloadDim}`}>
                <div className="text-[17px] font-bold text-ink">{entry.source}</div>
                <div className="mt-3 grid gap-x-8 gap-y-2 text-sm text-ink-2 md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                  <div className="grid grid-cols-2 gap-2">
                    <DatasetFunnelRows entry={entry} />
                  </div>
                  <div className="grid grid-cols-2 content-start gap-2">
                    <span className="font-medium text-ink">Papers and citations</span>
                    <span />
                    <span>Primary papers</span>
                    <span className="text-right">{formatNumber(entry.distinct_mapped_primary_papers)}</span>
                    <span>Citation edges</span>
                    <span className="text-right">{formatNumber(entry.citation_edges)}</span>
                    <span>Contexts</span>
                    <span className="text-right">{formatNumber(entry.citations_with_contexts)}</span>
                    <span>Classified edges</span>
                    <span className="text-right">{formatNumber(entry.classified_edges)}</span>
                  </div>
                </div>
              </div>
            ))}
            {!summary?.by_source?.some((entry) => entry.source === sourceFilter) ? (
              loading || !summary ? (
                <SourceCardSkeleton source={sourceFilter} />
              ) : (
                <div className="rounded-md border border-line bg-surface p-4 text-sm text-muted shadow-card">
                  No paper-mapping tables for {sourceFilter} yet. Run its paper-mapping DAG.
                </div>
              )
            ) : null}
          </section>
        )}

        {summary ? (
          <div className={reloadDim}>
            <ClassificationDistribution
              progress={classificationProgress(summary.summary.citation_edges, summary.by_classification)}
            />
          </div>
        ) : firstLoad ? (
          <ClassificationDistributionSkeleton />
        ) : null}

        <div className="mt-6 grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
          {/* min-w-0: on phones the table scrolls inside this card instead of widening the page. */}
          <section className="min-w-0 rounded-md border border-line bg-surface shadow-card">
            <div className="flex items-center justify-between border-b border-line-2 px-4 py-3">
              <div>
                <h2 className="m-0 border-b-[3px] border-line-rule pb-2 text-[17px] font-bold text-ink">Mapped Datasets</h2>
                <p className="text-sm text-muted">
                  {firstLoad ? 'Loading datasets…' : `${formatNumber(datasetCount)} datasets in current view`}
                  {classificationBucketFilter ? (
                    <>
                      {' '}
                      <span className="text-ink-2">
                        · filtered by classification:{' '}
                        <span className="font-medium">{classificationBucketFilter.replace(/_/g, ' ')}</span>
                      </span>
                      <button
                        type="button"
                        onClick={() => setClassificationBucketFilter(null)}
                        className="ml-2 text-link hover:underline"
                      >
                        Clear
                      </button>
                    </>
                  ) : null}
                </p>
              </div>
              {loading ? (
                <span className="flex items-center gap-2 text-sm text-muted">
                  <Spinner />
                  Loading…
                </span>
              ) : null}
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-line-2 text-left text-sm">
                <thead className="bg-panel-head text-[11px] font-bold uppercase tracking-[0.06em] text-muted">
                  <tr>
                    <SortableHeader label="Source" active={sortBy === 'source'} order={sortOrder} onClick={() => toggleSort('source')} />
                    <SortableHeader label="Dataset" active={sortBy === 'id'} order={sortOrder} onClick={() => toggleSort('id')} />
                    <SortableHeader label="Title" active={sortBy === 'title'} order={sortOrder} onClick={() => toggleSort('title')} />
                    <SortableHeader
                      label="Mapped Papers"
                      active={sortBy === 'mapped_papers'}
                      order={sortOrder}
                      onClick={() => toggleSort('mapped_papers')}
                    />
                    <SortableHeader
                      label="Citation Edges"
                      active={sortBy === 'citation_edges'}
                      order={sortOrder}
                      onClick={() => toggleSort('citation_edges')}
                    />
                    <SortableHeader
                      label="Contexts"
                      active={sortBy === 'contexts_extracted'}
                      order={sortOrder}
                      onClick={() => toggleSort('contexts_extracted')}
                    />
                  </tr>
                </thead>
                <tbody
                  aria-busy={loading}
                  className={`divide-y divide-line-3 ${reloadDim}`}
                >
                  {firstLoad ? <TableSkeletonRows rows={8} columns={6} /> : null}
                  {datasets.map((dataset) => {
                    const isSelected =
                      selectedDataset?.source === dataset.source && selectedDataset?.dataset_id === dataset.dataset_id;
                    return (
                      <tr
                        key={`${dataset.source}:${dataset.dataset_id}`}
                        data-opens-panel
                        className={`cursor-pointer transition hover:bg-panel-head ${isSelected ? 'bg-reuse-tint' : ''}`}
                        onClick={() => openPanel(dataset)}
                      >
                        <td className="px-4 py-3 align-top">
                          <span className="rounded border border-chip-border bg-chip-bg px-2 py-0.5 text-xs text-chip-text">{dataset.source}</span>
                        </td>
                        <td className="px-4 py-3 align-top font-mono text-xs text-ink-2">{dataset.dataset_id}</td>
                        <td className="px-4 py-3 align-top">
                          <Link
                            to={datasetDetailPath(dataset.source, dataset.dataset_id)}
                            className="font-medium text-ink hover:text-link hover:underline transition-colors"
                            onClick={(e) => e.stopPropagation()}
                          >
                            {truncate(dataset.dataset_title, 80) || 'Untitled dataset'}
                          </Link>
                          <div className="mt-1 text-xs text-muted">{truncate(dataset.dataset_description, 110) || 'No description'}</div>
                        </td>
                        <td className="px-4 py-3 align-top">{formatNumber(dataset.mapped_papers_count)}</td>
                        <td className="px-4 py-3 align-top">{formatNumber(dataset.citation_edges_count)}</td>
                        <td className="px-4 py-3 align-top">{formatNumber(dataset.contexts_extracted_count)}</td>
                      </tr>
                    );
                  })}
                  {!loading && datasets.length === 0 ? (
                    <tr>
                      <td className="px-4 py-6 text-sm text-muted" colSpan={6}>
                        No mapped datasets match the current filters.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between border-t border-line-2 px-4 py-3 text-sm text-ink-2">
              <span>
                Page {page} of {totalPages}
              </span>
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={page <= 1}
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                  className="h-8 rounded-[5px] border border-line bg-surface px-3 text-[13px] font-bold text-ink-2 hover:bg-panel disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Previous
                </button>
                <button
                  type="button"
                  disabled={page >= totalPages}
                  onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                  className="h-8 rounded-[5px] border border-line bg-surface px-3 text-[13px] font-bold text-ink-2 hover:bg-panel disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Next
                </button>
              </div>
            </div>
          </section>

          {/* Phones: `contents` lets the Snapshot join the single column on its own,
              so the Snapshot (the filter) can sit above the table.
              Desktop: an ordinary sidebar. */}
          <aside className="contents lg:block lg:space-y-6">
            <section className="order-first rounded-md border border-line bg-surface p-4 shadow-card lg:order-none">
              <h2 className="m-0 border-b-[3px] border-line-rule pb-2 text-[17px] font-bold text-ink">Classification Snapshot</h2>
              <p className="mt-1 text-xs text-muted">
                Click a row to show only datasets that have at least one edge in that bucket. Click again to clear.
              </p>
              <div className={`mt-3 space-y-2 text-sm ${reloadDim}`}>
                {firstLoad ? (
                  [0, 1, 2, 3].map((i) => <Skeleton key={i} className="h-9 rounded" />)
                ) : classificationBreakdown.length ? (
                  classificationBreakdown.map(([bucket, count]) => {
                    const active = classificationBucketFilter === bucket;
                    return (
                      <button
                        key={bucket}
                        type="button"
                        onClick={() =>
                          setClassificationBucketFilter((prev) => (prev === bucket ? null : bucket))
                        }
                        className={`flex w-full items-center justify-between rounded px-3 py-2 text-left transition ${
                          active
                            ? 'bg-reuse-tint ring-2 ring-reuse-soft'
                            : 'bg-panel-head hover:bg-panel'
                        }`}
                      >
                        <span className="flex items-center gap-2 text-ink-2">
                          <span className={`inline-block h-2.5 w-2.5 ${statusFillClass(bucket)}`} aria-hidden="true" />
                          {statusLabel(bucket)}
                        </span>
                        <span className="font-medium text-ink">{formatNumber(count)}</span>
                      </button>
                    );
                  })
                ) : (
                  <p className="text-muted">No classification rows yet. Run the paper_reuse_classification DAG after paper mapping.</p>
                )}
              </div>
            </section>
          </aside>
        </div>

        {selectedDataset ? (
        <aside
          ref={panelRef}
          role="dialog"
          aria-modal="false"
          aria-label={`Dataset drill-down: ${selectedDataset.source} ${selectedDataset.dataset_id}`}
          className={`fixed inset-y-0 right-0 z-40 flex w-full max-w-3xl flex-col border-l border-line bg-canvas shadow-pop motion-reduce:animate-none ${
            panelClosing ? 'animate-panel-out' : 'animate-panel-in'
          }`}
        >
          <div className="flex shrink-0 items-start justify-between gap-3 border-b border-line-2 bg-surface px-4 py-3">
            <div>
              <h2 className="m-0 border-b-[3px] border-line-rule pb-2 text-[17px] font-bold text-ink">Dataset Drill-down</h2>
              <p className="text-sm text-muted">
                {selectedDataset.source} {selectedDataset.dataset_id}
              </p>
            </div>
            <button
              type="button"
              onClick={closePanel}
              aria-label="Close drill-down"
              className="rounded px-2 py-1 text-lg leading-none text-muted hover:bg-panel hover:text-ink"
            >
              ✕
            </button>
          </div>
          <div className="flex min-h-0 flex-1 flex-col overflow-y-auto p-4">
            {detailLoading ? (
              <p className="flex items-center gap-2 text-sm text-muted">
                <Spinner />
                Loading dataset detail…
              </p>
            ) : null}
            {!detailLoading && datasetDetail ? (
              <div className="flex min-h-0 flex-1 flex-col gap-6">
                <div className="shrink-0 rounded-[5px] border border-line-2 bg-surface p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="text-[11px] font-bold uppercase tracking-[0.06em] text-muted">{datasetDetail.dataset.source}</div>
                      <h3 className="mt-1 text-lg font-bold">
                        <Link
                          to={datasetDetailPath(datasetDetail.dataset.source, datasetDetail.dataset.dataset_id)}
                          className="hover:text-link hover:underline transition-colors"
                        >
                          {datasetDetail.dataset.dataset_title || datasetDetail.dataset.dataset_id}
                        </Link>
                      </h3>
                      <p className="mt-2 max-w-3xl text-sm text-ink-2">
                        {datasetDetail.dataset.dataset_description || 'No description available.'}
                      </p>
                    </div>
                    {datasetDetail.dataset.url ? (
                      <a
                        href={datasetDetail.dataset.url}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex h-[38px] items-center rounded-[5px] border border-accent bg-surface px-4 text-sm font-bold text-ink no-underline hover:bg-panel hover:text-ink hover:no-underline"
                      >
                        Open dataset
                      </a>
                    ) : null}
                  </div>
                  <div className="mt-4 grid grid-cols-2 gap-3 sm:grid-cols-4">
                    <MiniStat label="Mapped primary papers" value={formatNumber(datasetDetail.dataset.mapped_papers_count)} />
                    <MiniStat label="Citation edges" value={formatNumber(datasetDetail.dataset.citation_edges_count)} />
                    <MiniStat label="Contexts extracted" value={formatNumber(datasetDetail.dataset.contexts_extracted_count)} />
                    <MiniStat label="Latest citing date" value={formatDate(datasetDetail.dataset.latest_citing_publication_date)} />
                  </div>
                </div>

                <div className="flex min-h-0 flex-1 flex-col gap-6">
                  <section className="shrink-0">
                    <h4 className="mb-3 text-xs font-bold uppercase tracking-[0.06em] text-muted">
                      Primary Papers <span className="font-normal normal-case text-muted">({formatNumber(datasetDetail.primary_papers.length)})</span>
                    </h4>
                    {/* Own scroller, so a long list does not push Citing Papers out of view. */}
                    <div className="max-h-[20vh] space-y-3 overflow-y-auto overscroll-contain pr-1">
                      {datasetDetail.primary_papers.map((paper) => (
                        <div key={paper.paper_doi} className="rounded-[5px] border border-line-2 bg-surface p-4">
                          <div className="flex flex-wrap items-start justify-between gap-2">
                            <div>
                              <div className="font-medium text-ink">{paper.paper_title || paper.paper_doi}</div>
                              <div className="mt-1 font-mono text-xs text-muted">{paper.paper_doi}</div>
                            </div>
                            <span className="rounded bg-neither-tint px-2 py-1 text-xs font-bold text-neither-ink">
                              {paper.relation_type || 'mapped'}
                            </span>
                          </div>
                          <div className="mt-3 grid grid-cols-2 gap-2 text-sm text-ink-2">
                            <span>Publication date</span>
                            <span className="text-right">{formatDate(paper.publication_date)}</span>
                            <span>Citing papers</span>
                            <span className="text-right">{formatNumber(paper.citing_papers_count)}</span>
                            <span>Contexts on edges</span>
                            <span className="text-right">{formatNumber(paper.citations_with_contexts_count)}</span>
                            <span>Placeholder classifications</span>
                            <span className="text-right">{formatNumber(paper.placeholder_classification_edges_count)}</span>
                          </div>
                        </div>
                      ))}
                      {datasetDetail.primary_papers.length === 0 ? <p className="text-sm text-muted">No primary papers found.</p> : null}
                    </div>
                  </section>

                  {/* Fills the rest of the panel and scrolls on its own, like Primary Papers. */}
                  <section className="flex min-h-[14rem] flex-1 flex-col">
                    <h4 className="text-xs font-bold uppercase tracking-[0.06em] text-muted">
                      Citing Papers <span className="font-normal normal-case text-muted">({formatNumber(citationsTotal)})</span>
                    </h4>
                    <p className="mb-3 mt-1 text-xs text-muted">
                      Showing {formatNumber(citationsPreview.length)} of {formatNumber(citationsTotal)}, classified papers first
                      (Reuse, then other labels), then newest.
                    </p>
                    <div className="min-h-0 flex-1 space-y-3 overflow-y-auto overscroll-contain pr-1">
                      {citationsPreview.map((citation) => {
                        const firstContext = citation.citation_contexts?.[0]?.context;
                        const conf = confidenceShort(citation.confidence);
                        const typeLabel = reuseTypeLabel(citation.reuse_type, citation.reuse_type_other);
                        const modalities = reusedModalities(citation.reused_modalities);
                        const quote = primaryQuote(citation.evidence_quotes);
                        const hallucinated = (citation.hallucinated_quote_count ?? 0) > 0;
                        return (
                          <div key={`${citation.primary_paper_doi}:${citation.citing_paper_doi}`} className="rounded-[5px] border border-line-2 bg-surface p-4">
                            <div className="flex flex-wrap items-start justify-between gap-2">
                              <div>
                                <div className="font-medium text-ink">{citation.citing_paper_title || citation.citing_paper_doi}</div>
                                <div className="mt-1 font-mono text-xs text-muted">{citation.citing_paper_doi}</div>
                              </div>
                              <div className="flex items-center gap-2">
                                <span
                                  className={`rounded px-2 py-1 text-xs font-bold ring-1 ${statusBadgeClass(citation.classification_status)}`}
                                  title={citation.mode ? `${citation.mode} mode, prompt v${citation.prompt_version ?? '?'}` : undefined}
                                >
                                  {statusLabel(citation.classification_status)}
                                </span>
                                {citation.confidence != null && citation.confidence > 0 && (
                                  <span className={`text-xs font-medium ${conf.color}`}>
                                    {conf.text}
                                  </span>
                                )}
                              </div>
                            </div>
                            {(typeLabel || modalities.length > 0 || citation.same_lab === true || citation.source_archive) && (
                              <div className="mt-2 flex flex-wrap items-center gap-1.5">
                                {typeLabel && (
                                  <span className="rounded bg-tan-tint px-2 py-0.5 text-[11px] font-bold text-tan-ink">
                                    {typeLabel}
                                  </span>
                                )}
                                {modalities.map((m) => (
                                  <span key={m} className="rounded border border-chip-border bg-chip-bg px-2 py-0.5 text-[11px] text-chip-text">
                                    {modalityLabel(m)}
                                  </span>
                                ))}
                                {citation.same_lab === true && (
                                  <span className="rounded bg-neither-tint px-2 py-0.5 text-[11px] font-bold text-neither-ink">
                                    Same lab
                                  </span>
                                )}
                                {citation.source_archive && (
                                  <span className="text-[11px] text-muted">via {citation.source_archive}</span>
                                )}
                              </div>
                            )}
                            <div className="mt-3 grid grid-cols-2 gap-2 text-sm text-ink-2">
                              <span>Primary paper</span>
                              <span className="text-right">{truncate(citation.primary_paper_title || citation.primary_paper_doi, 36)}</span>
                              <span>Citation date</span>
                              <span className="text-right">{formatDate(citation.citing_publication_date || citation.citing_publication_date_from_papers)}</span>
                              <span>Full text</span>
                              <span className="text-right">{citation.citing_text_status ? statusLabel(citation.citing_text_status) : '—'}</span>
                              <span>Contexts found</span>
                              <span className="text-right">{formatNumber(citation.citation_contexts?.length || 0)}</span>
                            </div>
                            {quote && (
                              <blockquote
                                className="mt-3 border-l-2 border-line-rule pl-3 text-sm text-ink-2"
                                title={quote.match_type ? `Verified against the paper text (${quote.match_type})` : undefined}
                              >
                                “{truncate(quote.quote, 320)}”
                              </blockquote>
                            )}
                            {citation.reasoning && (
                              <div className="mt-2 text-xs text-muted italic">
                                {truncate(citation.reasoning, 240)}
                              </div>
                            )}
                            {hallucinated && (
                              <div className="mt-1 text-xs text-danger-ink">
                                {citation.hallucinated_quote_count} quote{citation.hallucinated_quote_count === 1 ? '' : 's'} could not be matched word for word
                              </div>
                            )}
                            {!quote && (
                              <div className="mt-3 rounded-[5px] bg-panel p-3 text-sm text-ink-2">
                                {firstContext ? truncate(firstContext, 280) : 'No extracted citation context stored yet.'}
                              </div>
                            )}
                          </div>
                        );
                      })}
                      {citationsPreview.length === 0 ? <p className="text-sm text-muted">No citation rows found for this dataset.</p> : null}
                    </div>
                  </section>
                </div>
              </div>
            ) : null}
          </div>
        </aside>
        ) : null}
      </div>
    </div>
  );
}

function percent(share: number): string {
  if (share <= 0) return '0%';
  if (share < 0.001) return '<0.1%';
  return `${(share * 100).toFixed(share < 0.1 ? 1 : 0)}%`;
}

/**
 * Two stacked bars: how many citation edges have been attempted at all, and
 * how the attempted ones split by outcome. One bar over every edge would hide
 * the outcomes while most edges are still unclassified.
 */
function ClassificationDistribution({ progress }: { progress: ClassificationProgress }) {
  const [hovered, setHovered] = useState<string | null>(null);
  const { total, attempted, notYet, attemptedShare, outcomes } = progress;
  const segment = (key: string, share: number, fill: { color?: string; className?: string }, tip: string) => (
    <div
      key={key}
      role="img"
      aria-label={tip}
      title={tip}
      onMouseEnter={() => setHovered(key)}
      onMouseLeave={() => setHovered(null)}
      className={`h-full transition-opacity ${fill.className ?? ''}`}
      style={{
        width: `${share * 100}%`,
        minWidth: share > 0 ? 3 : 0,
        backgroundColor: fill.color,
        opacity: hovered && hovered !== key ? 0.45 : 1,
      }}
    />
  );

  return (
    <section className="mt-4 rounded-md border border-line bg-surface p-4 shadow-card">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="m-0 border-b-[3px] border-line-rule pb-2 text-[17px] font-bold text-ink">Classification Progress</h2>
        <p className="text-sm text-muted">
          <span className="font-bold text-ink">{formatNumber(attempted)}</span> of {formatNumber(total)} citation
          edges attempted ({percent(attemptedShare)}) · {formatNumber(notYet)} not yet classified
        </p>
      </div>

      <div className="mt-3 flex h-3 w-full gap-[2px] overflow-hidden bg-surface" aria-label="Attempted vs not yet classified">
        {segment('attempted', attemptedShare, { className: 'bg-primary' }, `Attempted: ${formatNumber(attempted)} (${percent(attemptedShare)})`)}
        {segment('not_yet', 1 - attemptedShare, { className: 'bg-line' }, `Not yet classified: ${formatNumber(notYet)} (${percent(1 - attemptedShare)})`)}
      </div>

      <h3 className="mt-5 text-[15.5px] font-bold text-ink">Outcome of attempted edges</h3>
      {outcomes.length ? (
        <>
          <div className="mt-2 flex h-6 w-full gap-[2px] overflow-hidden" aria-label="Outcome of attempted edges">
            {outcomes.map((o) =>
              segment(o.key, o.share, { className: o.fill }, `${o.label}: ${formatNumber(o.count)} (${percent(o.share)} of attempted)`),
            )}
          </div>
          <ul className="mt-3 flex flex-wrap gap-x-5 gap-y-2 text-sm">
            {outcomes.map((o) => (
              <li
                key={o.key}
                className="flex items-center gap-2"
                onMouseEnter={() => setHovered(o.key)}
                onMouseLeave={() => setHovered(null)}
              >
                <span className={`inline-block h-2.5 w-2.5 ${o.fill}`} aria-hidden="true" />
                <span className="text-ink-2">{o.label}</span>
                <span className="font-bold text-ink">{formatNumber(o.count)}</span>
                <span className="text-muted">{percent(o.share)}</span>
              </li>
            ))}
          </ul>
        </>
      ) : (
        <p className="mt-2 text-sm text-muted">Nothing classified yet. Run the paper_reuse_classification DAG.</p>
      )}
    </section>
  );
}

function SummaryCard({ title, value, hint, loading = false }: { title: string; value: string; hint?: string; loading?: boolean }) {
  return (
    <div className="rounded-md border border-line bg-surface p-4 shadow-card" aria-busy={loading}>
      <div className="text-[11px] font-bold uppercase tracking-[0.06em] text-muted">{title}</div>
      {loading ? (
        <Skeleton className="mt-2 h-9 w-24 rounded" />
      ) : (
        <div className="mt-2 text-[32px] font-bold leading-[1.05] tabular-nums text-ink">{value}</div>
      )}
      {hint ? <div className="mt-1 text-xs text-muted">{hint}</div> : null}
    </div>
  );
}

/**
 * One archive's dataset funnel: ingested (junk removed) split into mapped /
 * no paper found / pending, with the junk count underneath. Shows nothing
 * until the DAGs have written dataset_status.
 */
function DatasetFunnelRows({ entry }: { entry: PaperMappingSourceSummary }) {
  if (typeof entry.ingested_datasets !== 'number') {
    return (
      <>
        <span>Datasets</span>
        <span className="text-right">{formatNumber(entry.datasets_with_mapped_papers)}</span>
      </>
    );
  }
  const ingested = entry.ingested_datasets;
  const mapped = entry.datasets_with_mapped_papers;
  const noPaper = entry.no_paper_datasets ?? 0;
  const pending = entry.pending_datasets ?? 0;
  const pct = (n: number) => (ingested > 0 ? `${(100 * n) / ingested}%` : '0%');
  const share = (n: number) => (ingested > 0 ? ` (${Math.round((100 * n) / ingested)}%)` : '');
  const junkReasons = Object.entries(entry.junk_reasons || {})
    .map(([reason, n]) => `${reason.replace(/_/g, ' ').replace(/:/g, ': ')} ${formatNumber(n)}`)
    .join(', ');
  return (
    <>
      <span className="font-medium text-ink">Ingested datasets</span>
      <span className="text-right font-medium text-ink">{formatNumber(ingested)}</span>
      <div
        className="col-span-2 flex h-2 overflow-hidden rounded-full bg-line-2"
        role="img"
        aria-label={`${formatNumber(mapped)} mapped, ${formatNumber(noPaper)} no paper found, ${formatNumber(pending)} pending`}
      >
        <div className="bg-reuse" style={{ width: pct(mapped) }} />
        <div className="bg-bar" style={{ width: pct(noPaper) }} />
        <div className="bg-mention" style={{ width: pct(pending) }} />
      </div>
      <span className="flex items-center gap-2">
        <span className="inline-block h-2.5 w-2.5 rounded-full bg-reuse" aria-hidden="true" />
        Mapped
      </span>
      <span className="text-right">
        {formatNumber(mapped)}
        <span className="text-muted">{share(mapped)}</span>
      </span>
      <span className="flex items-center gap-2">
        <span className="inline-block h-2.5 w-2.5 rounded-full bg-bar" aria-hidden="true" />
        No paper found
      </span>
      <span className="text-right">
        {formatNumber(noPaper)}
        <span className="text-muted">{share(noPaper)}</span>
      </span>
      <span className="flex items-center gap-2">
        <span className="inline-block h-2.5 w-2.5 rounded-full bg-mention" aria-hidden="true" />
        Pending
      </span>
      <span className="text-right">
        {formatNumber(pending)}
        <span className="text-muted">{share(pending)}</span>
      </span>
      {typeof entry.never_published_datasets === 'number' ? (
        <>
          <span className="pl-4 text-muted">of which never published (draft only)</span>
          <span className="text-right text-muted">{formatNumber(entry.never_published_datasets)}</span>
        </>
      ) : null}
      <span className="text-danger-ink" title={junkReasons || undefined}>
        Junk excluded
      </span>
      <span className="text-right text-danger-ink">{formatNumber(entry.junk_datasets)}</span>
      {junkReasons ? <span className="col-span-2 text-[11px] leading-snug text-muted">{junkReasons}</span> : null}
    </>
  );
}

function TableSkeletonRows({ rows, columns }: { rows: number; columns: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_, r) => (
        <tr key={r}>
          {Array.from({ length: columns }, (_, c) => (
            <td key={c} className="px-4 py-3 align-top">
              <Skeleton className={`h-4 rounded ${c === 2 ? 'w-48' : 'w-12'}`} />
              {c === 2 ? <Skeleton className="mt-2 h-3 w-64 rounded" /> : null}
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

/**
 * Same shape as the filtered archive's card (title, funnel column with its
 * bar, papers-and-citations column), so switching archives doesn't collapse
 * the card to one line and grow it back when the summary lands.
 */
function SourceCardSkeleton({ source }: { source: string }) {
  const row = (key: number, wide = false) => (
    <React.Fragment key={key}>
      <Skeleton className={`h-4 rounded ${wide ? 'w-40' : 'w-28'}`} />
      <Skeleton className="h-4 w-14 justify-self-end rounded" />
    </React.Fragment>
  );
  return (
    <div className="rounded-md border border-line bg-surface p-4 shadow-card" aria-busy="true">
      <div className="text-[17px] font-bold text-ink">{source}</div>
      <div className="mt-3 grid gap-x-8 gap-y-2 text-sm md:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
        <div className="grid grid-cols-2 gap-2">
          {row(0, true)}
          <Skeleton className="col-span-2 h-2 rounded-full" />
          {[1, 2, 3, 4].map((i) => row(i))}
          <Skeleton className="col-span-2 h-3 w-full rounded" />
        </div>
        <div className="grid grid-cols-2 content-start gap-2">
          {[5, 6, 7, 8, 9].map((i) => row(i))}
        </div>
      </div>
    </div>
  );
}

function ClassificationDistributionSkeleton() {
  return (
    <section className="mt-4 rounded-md border border-line bg-surface p-4 shadow-card" aria-busy="true">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="m-0 border-b-[3px] border-line-rule pb-2 text-[17px] font-bold text-ink">Classification Progress</h2>
        <Skeleton className="h-4 w-72 max-w-full rounded" />
      </div>
      <Skeleton className="mt-3 h-3 w-full rounded" />
      <h3 className="mt-5 text-[15.5px] font-bold text-ink">Outcome of attempted edges</h3>
      <Skeleton className="mt-2 h-6 w-full rounded" />
    </section>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[5px] border border-line-2 bg-surface p-3">
      <div className="text-[11px] font-bold uppercase tracking-[0.06em] text-muted">{label}</div>
      <div className="mt-2 text-sm font-bold text-ink">{value}</div>
    </div>
  );
}

function SortableHeader({
  label,
  active,
  order,
  onClick,
}: {
  label: string;
  active: boolean;
  order: 'asc' | 'desc';
  onClick: () => void;
}) {
  return (
    <th className="px-4 py-3 font-bold">
      <button type="button" onClick={onClick} className="inline-flex items-center gap-1 hover:text-ink">
        <span>{label}</span>
        <span className={`text-xs ${active ? 'text-ink' : 'text-muted'}`}>{active ? (order === 'asc' ? '▲' : '▼') : '⇅'}</span>
      </button>
    </th>
  );
}
