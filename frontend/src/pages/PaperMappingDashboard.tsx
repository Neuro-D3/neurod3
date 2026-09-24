import React, { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Link } from 'react-router-dom';
import {
  datasetDetailPath,
  fetchPaperMappingCitations,
  fetchPaperMappingDatasetDetail,
  fetchPaperMappingDatasets,
  fetchPaperMappingSummary,
  PaperMappingCitation,
  PaperMappingDatasetDetail,
  PaperMappingDatasetRow,
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
  if (!value) return '—';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleDateString();
}

function formatNumber(value?: number | null): string {
  if (typeof value !== 'number') return '0';
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

  return (
    <div className="min-h-screen bg-slate-100 text-slate-900">
      <div className="mx-auto max-w-7xl px-4 py-8 sm:px-6 lg:px-8">
        <div className="mb-6">
          <h1 className="text-3xl font-semibold tracking-tight">Internal Paper Mapping Dashboard</h1>
          <p className="mt-2 max-w-3xl text-sm text-slate-600">
            Review mapped primary papers, citation enrichment coverage, and whole-paper reuse classifications (REUSE / MENTION / NEITHER, with verified quotes) across DANDI, OpenNeuro, CRCNS, and SPARC.
          </p>
        </div>

        <div className="mb-6 flex flex-col gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm sm:flex-row sm:items-center sm:justify-between">
          <div className="flex flex-wrap items-center gap-2">
            {(['all', 'DANDI', 'OpenNeuro', 'CRCNS', 'SPARC'] as SourceFilter[]).map((option) => (
              <button
                key={option}
                type="button"
                onClick={() => setSourceFilter(option)}
                className={`rounded-full px-4 py-2 text-sm font-medium transition ${
                  sourceFilter === option
                    ? 'bg-slate-900 text-white'
                    : 'bg-slate-100 text-slate-700 hover:bg-slate-200'
                }`}
              >
                {option === 'all' ? 'All Sources' : option}
              </button>
            ))}
          </div>
          <div className="flex w-full max-w-md items-center rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
            <input
              className="w-full bg-transparent text-sm outline-none"
              placeholder="Search dataset id, title, or description"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
            />
          </div>
        </div>

        {error ? (
          <div className="mb-6 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div>
        ) : null}

        <div className="grid gap-4 md:grid-cols-3">
          <SummaryCard title="Datasets With Mapped Papers" value={formatNumber(summary?.summary.datasets_with_mapped_papers)} />
          <SummaryCard title="Distinct Primary Papers" value={formatNumber(summary?.summary.distinct_mapped_primary_papers)} />
          <SummaryCard title="Citation Edges" value={formatNumber(summary?.summary.citation_edges)} />
        </div>

        {summary ? (
          <ClassificationDistribution
            progress={classificationProgress(summary.summary.citation_edges, summary.by_classification)}
          />
        ) : null}

        <div className="mt-6 grid gap-6 lg:grid-cols-[minmax(0,2fr)_minmax(320px,1fr)]">
          <section className="rounded-2xl border border-slate-200 bg-white shadow-sm">
            <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
              <div>
                <h2 className="text-base font-semibold">Mapped Datasets</h2>
                <p className="text-sm text-slate-500">
                  {formatNumber(datasetCount)} datasets in current view
                  {classificationBucketFilter ? (
                    <>
                      {' '}
                      <span className="text-slate-700">
                        · filtered by classification:{' '}
                        <span className="font-medium">{classificationBucketFilter.replace(/_/g, ' ')}</span>
                      </span>
                      <button
                        type="button"
                        onClick={() => setClassificationBucketFilter(null)}
                        className="ml-2 text-blue-600 hover:underline"
                      >
                        Clear
                      </button>
                    </>
                  ) : null}
                </p>
              </div>
              {loading ? <span className="text-sm text-slate-500">Loading…</span> : null}
            </div>

            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-slate-200 text-left text-sm">
                <thead className="bg-slate-50 text-slate-600">
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
                <tbody className="divide-y divide-slate-100">
                  {datasets.map((dataset) => {
                    const isSelected =
                      selectedDataset?.source === dataset.source && selectedDataset?.dataset_id === dataset.dataset_id;
                    return (
                      <tr
                        key={`${dataset.source}:${dataset.dataset_id}`}
                        className={`cursor-pointer transition hover:bg-slate-50 ${isSelected ? 'bg-slate-50' : ''}`}
                        onClick={() => openPanel(dataset)}
                      >
                        <td className="px-4 py-3 align-top">
                          <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs font-medium text-slate-700">{dataset.source}</span>
                        </td>
                        <td className="px-4 py-3 align-top font-mono text-xs text-slate-700">{dataset.dataset_id}</td>
                        <td className="px-4 py-3 align-top">
                          <Link
                            to={datasetDetailPath(dataset.source, dataset.dataset_id)}
                            className="font-medium text-slate-900 hover:text-blue-600 hover:underline transition-colors"
                            onClick={(e) => e.stopPropagation()}
                          >
                            {truncate(dataset.dataset_title, 80) || 'Untitled dataset'}
                          </Link>
                          <div className="mt-1 text-xs text-slate-500">{truncate(dataset.dataset_description, 110) || 'No description'}</div>
                        </td>
                        <td className="px-4 py-3 align-top">{formatNumber(dataset.mapped_papers_count)}</td>
                        <td className="px-4 py-3 align-top">{formatNumber(dataset.citation_edges_count)}</td>
                        <td className="px-4 py-3 align-top">{formatNumber(dataset.contexts_extracted_count)}</td>
                      </tr>
                    );
                  })}
                  {!loading && datasets.length === 0 ? (
                    <tr>
                      <td className="px-4 py-6 text-sm text-slate-500" colSpan={6}>
                        No mapped datasets match the current filters.
                      </td>
                    </tr>
                  ) : null}
                </tbody>
              </table>
            </div>

            <div className="flex items-center justify-between border-t border-slate-200 px-4 py-3 text-sm text-slate-600">
              <span>
                Page {page} of {totalPages}
              </span>
              <div className="flex gap-2">
                <button
                  type="button"
                  disabled={page <= 1}
                  onClick={() => setPage((prev) => Math.max(1, prev - 1))}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Previous
                </button>
                <button
                  type="button"
                  disabled={page >= totalPages}
                  onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))}
                  className="rounded-lg border border-slate-200 px-3 py-1.5 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  Next
                </button>
              </div>
            </div>
          </section>

          <aside className="space-y-6">
            <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <h2 className="text-base font-semibold">Classification Snapshot</h2>
              <p className="mt-1 text-xs text-slate-500">
                Click a row to show only datasets that have at least one edge in that bucket. Click again to clear.
              </p>
              <div className="mt-3 space-y-2 text-sm">
                {classificationBreakdown.length ? (
                  classificationBreakdown.map(([bucket, count]) => {
                    const active = classificationBucketFilter === bucket;
                    return (
                      <button
                        key={bucket}
                        type="button"
                        onClick={() =>
                          setClassificationBucketFilter((prev) => (prev === bucket ? null : bucket))
                        }
                        className={`flex w-full items-center justify-between rounded-lg px-3 py-2 text-left transition ${
                          active
                            ? 'bg-blue-100 ring-2 ring-blue-300'
                            : 'bg-slate-50 hover:bg-slate-100'
                        }`}
                      >
                        <span className="flex items-center gap-2 text-slate-700">
                          <span className={`inline-block h-2.5 w-2.5 rounded-full ring-1 ${statusBadgeClass(bucket)}`} aria-hidden="true" />
                          {statusLabel(bucket)}
                        </span>
                        <span className="font-medium text-slate-900">{formatNumber(count)}</span>
                      </button>
                    );
                  })
                ) : (
                  <p className="text-slate-500">No classification rows yet. Run the paper_reuse_classification DAG after paper mapping.</p>
                )}
              </div>
            </section>

            <section className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
              <h2 className="text-base font-semibold">Source Breakdown</h2>
              <div className="mt-3 space-y-3 text-sm">
                {(summary?.by_source || []).map((entry) => (
                  <div key={entry.source} className="rounded-xl bg-slate-50 p-3">
                    <div className="font-medium text-slate-900">{entry.source}</div>
                    <div className="mt-2 grid grid-cols-2 gap-2 text-slate-600">
                      <span>Datasets</span>
                      <span className="text-right">{formatNumber(entry.datasets_with_mapped_papers)}</span>
                      <span>Primary papers</span>
                      <span className="text-right">{formatNumber(entry.distinct_mapped_primary_papers)}</span>
                      <span>Citation edges</span>
                      <span className="text-right">{formatNumber(entry.citation_edges)}</span>
                      <span>Contexts</span>
                      <span className="text-right">{formatNumber(entry.citations_with_contexts)}</span>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          </aside>
        </div>

        {selectedDataset ? (
        <aside
          role="dialog"
          aria-modal="false"
          aria-label={`Dataset drill-down: ${selectedDataset.source} ${selectedDataset.dataset_id}`}
          className={`fixed inset-y-0 right-0 z-40 flex w-full max-w-3xl flex-col border-l border-slate-300 bg-slate-50 shadow-2xl motion-reduce:animate-none ${
            panelClosing ? 'animate-panel-out' : 'animate-panel-in'
          }`}
        >
          <div className="flex shrink-0 items-start justify-between gap-3 border-b border-slate-200 bg-white px-4 py-3">
            <div>
              <h2 className="text-base font-semibold">Dataset Drill-down</h2>
              <p className="text-sm text-slate-500">
                {selectedDataset.source} {selectedDataset.dataset_id}
              </p>
            </div>
            <button
              type="button"
              onClick={closePanel}
              aria-label="Close drill-down"
              className="rounded-lg px-2 py-1 text-lg leading-none text-slate-500 hover:bg-slate-100 hover:text-slate-900"
            >
              ✕
            </button>
          </div>
          <div className="flex min-h-0 flex-1 flex-col overflow-y-auto p-4">
            {detailLoading ? <p className="text-sm text-slate-500">Loading dataset detail…</p> : null}
            {!detailLoading && datasetDetail ? (
              <div className="flex min-h-0 flex-1 flex-col gap-6">
                <div className="shrink-0 rounded-xl border border-slate-200 bg-white p-4">
                  <div className="flex flex-wrap items-start justify-between gap-3">
                    <div>
                      <div className="text-xs uppercase tracking-wide text-slate-500">{datasetDetail.dataset.source}</div>
                      <h3 className="mt-1 text-lg font-semibold">
                        <Link
                          to={datasetDetailPath(datasetDetail.dataset.source, datasetDetail.dataset.dataset_id)}
                          className="hover:text-blue-600 hover:underline transition-colors"
                        >
                          {datasetDetail.dataset.dataset_title || datasetDetail.dataset.dataset_id}
                        </Link>
                      </h3>
                      <p className="mt-2 max-w-3xl text-sm text-slate-600">
                        {datasetDetail.dataset.dataset_description || 'No description available.'}
                      </p>
                    </div>
                    {datasetDetail.dataset.url ? (
                      <a
                        href={datasetDetail.dataset.url}
                        target="_blank"
                        rel="noreferrer"
                        className="rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100"
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
                    <h4 className="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500">
                      Primary Papers <span className="font-normal normal-case text-slate-400">({formatNumber(datasetDetail.primary_papers.length)})</span>
                    </h4>
                    {/* Own scroller, so a long list does not push Citing Papers out of view. */}
                    <div className="max-h-[20vh] space-y-3 overflow-y-auto overscroll-contain pr-1">
                      {datasetDetail.primary_papers.map((paper) => (
                        <div key={paper.paper_doi} className="rounded-xl border border-slate-200 bg-white p-4">
                          <div className="flex flex-wrap items-start justify-between gap-2">
                            <div>
                              <div className="font-medium text-slate-900">{paper.paper_title || paper.paper_doi}</div>
                              <div className="mt-1 font-mono text-xs text-slate-500">{paper.paper_doi}</div>
                            </div>
                            <span className="rounded-full bg-slate-100 px-2.5 py-1 text-xs text-slate-700">
                              {paper.relation_type || 'mapped'}
                            </span>
                          </div>
                          <div className="mt-3 grid grid-cols-2 gap-2 text-sm text-slate-600">
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
                      {datasetDetail.primary_papers.length === 0 ? <p className="text-sm text-slate-500">No primary papers found.</p> : null}
                    </div>
                  </section>

                  {/* Fills the rest of the panel and scrolls on its own, like Primary Papers. */}
                  <section className="flex min-h-[14rem] flex-1 flex-col">
                    <h4 className="text-sm font-semibold uppercase tracking-wide text-slate-500">
                      Citing Papers <span className="font-normal normal-case text-slate-400">({formatNumber(citationsTotal)})</span>
                    </h4>
                    <p className="mb-3 mt-1 text-xs text-slate-500">
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
                          <div key={`${citation.primary_paper_doi}:${citation.citing_paper_doi}`} className="rounded-xl border border-slate-200 bg-white p-4">
                            <div className="flex flex-wrap items-start justify-between gap-2">
                              <div>
                                <div className="font-medium text-slate-900">{citation.citing_paper_title || citation.citing_paper_doi}</div>
                                <div className="mt-1 font-mono text-xs text-slate-500">{citation.citing_paper_doi}</div>
                              </div>
                              <div className="flex items-center gap-2">
                                <span
                                  className={`rounded-full px-2.5 py-1 text-xs font-medium ring-1 ${statusBadgeClass(citation.classification_status)}`}
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
                                  <span className="rounded-full bg-violet-50 px-2 py-0.5 text-[11px] font-semibold uppercase tracking-wide text-violet-700 ring-1 ring-violet-200">
                                    {typeLabel}
                                  </span>
                                )}
                                {modalities.map((m) => (
                                  <span key={m} className="rounded-full bg-blue-50 px-2 py-0.5 text-[11px] font-medium text-blue-700 ring-1 ring-blue-100">
                                    {modalityLabel(m)}
                                  </span>
                                ))}
                                {citation.same_lab === true && (
                                  <span className="rounded-full bg-amber-50 px-2 py-0.5 text-[11px] font-medium text-amber-700 ring-1 ring-amber-200">
                                    Same lab
                                  </span>
                                )}
                                {citation.source_archive && (
                                  <span className="text-[11px] text-slate-400">via {citation.source_archive}</span>
                                )}
                              </div>
                            )}
                            <div className="mt-3 grid grid-cols-2 gap-2 text-sm text-slate-600">
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
                                className="mt-3 border-l-2 border-slate-300 pl-3 text-sm text-slate-700"
                                title={quote.match_type ? `Verified against the paper text (${quote.match_type})` : undefined}
                              >
                                “{truncate(quote.quote, 320)}”
                              </blockquote>
                            )}
                            {citation.reasoning && (
                              <div className="mt-2 text-xs text-slate-500 italic">
                                {truncate(citation.reasoning, 240)}
                              </div>
                            )}
                            {hallucinated && (
                              <div className="mt-1 text-xs text-rose-600">
                                {citation.hallucinated_quote_count} quote{citation.hallucinated_quote_count === 1 ? '' : 's'} not found in the paper text
                              </div>
                            )}
                            {!quote && (
                              <div className="mt-3 rounded-lg bg-slate-50 p-3 text-sm text-slate-700">
                                {firstContext ? truncate(firstContext, 280) : 'No extracted citation context stored yet.'}
                              </div>
                            )}
                          </div>
                        );
                      })}
                      {citationsPreview.length === 0 ? <p className="text-sm text-slate-500">No citation rows found for this dataset.</p> : null}
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
  const segment = (key: string, share: number, color: string, tip: string) => (
    <div
      key={key}
      role="img"
      aria-label={tip}
      title={tip}
      onMouseEnter={() => setHovered(key)}
      onMouseLeave={() => setHovered(null)}
      className="h-full transition-opacity first:rounded-l last:rounded-r"
      style={{
        width: `${share * 100}%`,
        minWidth: share > 0 ? 3 : 0,
        backgroundColor: color,
        opacity: hovered && hovered !== key ? 0.45 : 1,
      }}
    />
  );

  return (
    <section className="mt-4 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="flex flex-wrap items-baseline justify-between gap-2">
        <h2 className="text-base font-semibold">Classification Progress</h2>
        <p className="text-sm text-slate-500">
          <span className="font-semibold text-slate-900">{formatNumber(attempted)}</span> of {formatNumber(total)} citation
          edges attempted ({percent(attemptedShare)}) · {formatNumber(notYet)} not yet classified
        </p>
      </div>

      <div className="mt-3 flex h-3 w-full gap-[2px] overflow-hidden rounded bg-white" aria-label="Attempted vs not yet classified">
        {segment('attempted', attemptedShare, '#334155', `Attempted: ${formatNumber(attempted)} (${percent(attemptedShare)})`)}
        {segment('not_yet', 1 - attemptedShare, '#e2e8f0', `Not yet classified: ${formatNumber(notYet)} (${percent(1 - attemptedShare)})`)}
      </div>

      <h3 className="mt-5 text-sm font-medium text-slate-700">Outcome of attempted edges</h3>
      {outcomes.length ? (
        <>
          <div className="mt-2 flex h-6 w-full gap-[2px] overflow-hidden rounded" aria-label="Outcome of attempted edges">
            {outcomes.map((o) =>
              segment(o.key, o.share, o.color, `${o.label}: ${formatNumber(o.count)} (${percent(o.share)} of attempted)`),
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
                <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ backgroundColor: o.color }} aria-hidden="true" />
                <span className="text-slate-700">{o.label}</span>
                <span className="font-semibold text-slate-900">{formatNumber(o.count)}</span>
                <span className="text-slate-500">{percent(o.share)}</span>
              </li>
            ))}
          </ul>
        </>
      ) : (
        <p className="mt-2 text-sm text-slate-500">Nothing classified yet. Run the paper_reuse_classification DAG.</p>
      )}
    </section>
  );
}

function SummaryCard({ title, value }: { title: string; value: string }) {
  return (
    <div className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
      <div className="text-sm font-medium text-slate-500">{title}</div>
      <div className="mt-2 text-3xl font-semibold tracking-tight text-slate-900">{value}</div>
    </div>
  );
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-lg bg-white p-3 shadow-sm ring-1 ring-slate-200">
      <div className="text-xs uppercase tracking-wide text-slate-500">{label}</div>
      <div className="mt-2 text-sm font-semibold text-slate-900">{value}</div>
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
    <th className="px-4 py-3 font-medium">
      <button type="button" onClick={onClick} className="inline-flex items-center gap-1 hover:text-slate-900">
        <span>{label}</span>
        <span className={`text-xs ${active ? 'text-slate-900' : 'text-slate-400'}`}>{active ? (order === 'asc' ? '▲' : '▼') : '⇅'}</span>
      </button>
    </th>
  );
}
