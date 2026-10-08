import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import { fetchDatasetDetail, fetchDatasetMetrics } from '../services/api';
import type { DatasetDetailResponse, DatasetContributor, DatasetMetrics } from '../services/api';
import { PopulationIcon } from '../components/PopulationIcon';
import { DatasetImpactCard } from '../components/DatasetImpactCard';
import { DatasetPaperList } from '../components/DatasetPaperList';
import { formatPublishedDate, plural } from '../utils/reuseMetrics';

const SOURCE_COLORS: Record<string, string> = {
  CRCNS: 'bg-cyan-100 text-cyan-800',
  DANDI: 'bg-purple-100 text-purple-800',
  OpenNeuro: 'bg-green-100 text-green-800',
  SPARC: 'bg-amber-100 text-amber-800',
};

// Short label for each junk reason (utils/dataset_status.py); title_keyword:<word> shows the word.
const JUNK_REASON_LABELS: Record<string, string> = {
  empty: 'empty dandiset',
  find_reuse_test_id: 'test ID',
  placeholder_title: 'placeholder title',
  description_phrase: 'test description',
  title_is_id: 'title is ID',
  no_title: 'no title',
};

const junkReasonLabel = (reason?: string | null): string =>
  reason?.startsWith('title_keyword:')
    ? `"${reason.slice('title_keyword:'.length)}" in title`
    : JUNK_REASON_LABELS[reason ?? ''] ?? 'junk';

const AUTHOR_COLORS: [string, string][] = [
  ['#818cf8', '#6366f1'], ['#38bdf8', '#0ea5e9'], ['#34d399', '#10b981'],
  ['#fbbf24', '#f59e0b'], ['#f87171', '#ef4444'], ['#a78bfa', '#8b5cf6'],
  ['#2dd4bf', '#14b8a6'], ['#fb923c', '#f97316'], ['#f472b6', '#ec4899'],
  ['#22d3ee', '#06b6d4'], ['#c084fc', '#a855f7'], ['#a3e635', '#84cc16'],
  ['#fb7185', '#e11d48'], ['#67e8f9', '#0891b2'], ['#e879f9', '#d946ef'],
];

function ModalityChip({ label }: { label: string }) {
  const isAcronym = /[A-Z]{2,}/.test(label);
  return (
    <span className="inline-block rounded-full bg-blue-50/80 border border-blue-100 px-2.5 py-0.5 text-xs font-medium text-blue-700">
      {isAcronym ? label : label.toLowerCase()}
    </span>
  );
}

function decodedDatasetIdParam(raw: string | undefined): string {
  if (!raw) return '';
  try {
    return decodeURIComponent(raw);
  } catch {
    return raw;
  }
}

export default function DatasetDetailPage() {
  const { source: sourceParam, datasetId: datasetIdParam } = useParams<{ source: string; datasetId: string }>();
  const source = decodedDatasetIdParam(sourceParam);
  const datasetId = decodedDatasetIdParam(datasetIdParam);
  const navigate = useNavigate();
  const [data, setData] = useState<DatasetDetailResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [metrics, setMetrics] = useState<DatasetMetrics | null>(null);
  const [metricsError, setMetricsError] = useState<string | null>(null);

  useEffect(() => {
    if (!source || !datasetId) return;
    let cancelled = false;
    setLoading(true);
    setError(null);

    fetchDatasetDetail(source, datasetId)
      .then((res) => {
        if (!cancelled) setData(res);
      })
      .catch((err) => {
        if (!cancelled) setError(err.message);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => { cancelled = true; };
  }, [source, datasetId]);

  // Metrics load on their own so the page doesn't wait for them.
  useEffect(() => {
    if (!source || !datasetId) return;
    let cancelled = false;
    setMetrics(null);
    setMetricsError(null);

    fetchDatasetMetrics(source, datasetId)
      .then((res) => {
        if (!cancelled) setMetrics(res);
      })
      .catch((err) => {
        if (!cancelled) setMetricsError(err.message);
      });

    return () => { cancelled = true; };
  }, [source, datasetId]);

  const trackedMetrics = metrics?.tracked ? metrics : null;

  if (loading) {
    return (
      <div className="min-h-screen bg-slate-100 flex items-center justify-center">
        <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600" />
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="min-h-screen bg-slate-100 flex items-center justify-center px-4">
        <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-10 text-center max-w-md">
          <div className="text-5xl mb-4">😔</div>
          <h2 className="text-lg font-semibold text-slate-800 mb-2">
            {error?.includes('not found') ? 'Dataset Not Found' : 'Something went wrong'}
          </h2>
          <p className="text-sm text-slate-500 mb-6">{error}</p>
          <button
            type="button"
            onClick={() => navigate(-1)}
            aria-label="Back"
            className="inline-flex h-10 w-10 items-center justify-center rounded-full bg-blue-600 text-lg font-medium text-white hover:bg-blue-700 shadow-lg shadow-blue-200 transition-all"
          >
            <span aria-hidden="true">←</span>
          </button>
        </div>
      </div>
    );
  }

  const ds = data.dataset;
  const modalities = (ds.modality || '')
    .split(/[;,]/)
    .map((m) => m.trim())
    .filter(Boolean);
  const routeIdDiffersFromApi = Boolean(datasetId && datasetId !== ds.dataset_id);
  const showReuseSummary = Boolean(trackedMetrics && trackedMetrics.reuse_count + trackedMetrics.mention_count > 0);

  return (
    <div className="min-h-screen bg-slate-100 py-10 px-4">
      <div className="mx-auto max-w-7xl">
        <div className="flex flex-col lg:flex-row gap-6 items-start">
          {/* ─── Left column: dataset content ─── */}
          <div className="flex-1 min-w-0">
            {/* Header card: toolbar row + dataset tile */}
            <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-6 sm:p-8 mb-6">
              <div className="flex flex-wrap items-center gap-x-3 gap-y-2 mb-4">
                <button
                  type="button"
                  onClick={() => navigate(-1)}
                  aria-label="Back"
                  className="inline-flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-slate-50/90 border border-slate-200/80 shadow-sm text-base text-slate-600 hover:text-blue-600 hover:border-slate-300 hover:shadow transition-all"
                >
                  <span aria-hidden="true">←</span>
                </button>
                <span
                  className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-semibold shadow-sm ${SOURCE_COLORS[ds.source] || 'bg-slate-100 text-slate-700'}`}
                >
                  {ds.source}
                </span>
                {ds.dataset_status === 'excluded' && (
                  <span
                    className="inline-flex items-center rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-semibold text-amber-800 shadow-sm"
                    title={`Excluded as junk (${ds.dataset_status_reason ?? 'unknown reason'}). Hidden from the dataset list and counts.`}
                  >
                    Junk · {junkReasonLabel(ds.dataset_status_reason)}
                  </span>
                )}
                <span className="text-sm font-mono text-slate-600 tabular-nums">{ds.dataset_id}</span>
                {routeIdDiffersFromApi && (
                  <span className="text-sm font-mono text-slate-400" title="URL id">
                    {datasetId}
                  </span>
                )}
              </div>

              <div className="rounded-xl border border-slate-200/70 bg-white/55 p-5 sm:p-6 shadow-sm">
                <h1 className="text-2xl font-bold text-slate-900 leading-tight mb-4">
                  {ds.title}
                </h1>

                {/* Metadata bar */}
                <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm text-slate-500">
                  {ds.created_at && (
                    <span>Published {formatPublishedDate(ds.created_at, ds.created_at_precision)}</span>
                  )}
                  {ds.updated_at && (
                    <span>Updated {new Date(ds.updated_at).toLocaleDateString()}</span>
                  )}
                  {ds.num_subjects != null && ds.num_subjects > 0 && (
                    <span className="inline-flex items-center gap-1">
                      <PopulationIcon size={15} className="text-slate-400" />
                      {ds.num_subjects.toLocaleString()} {ds.source === 'OpenNeuro' ? 'participants' : 'subjects'}
                    </span>
                  )}
                  {ds.license && (
                    <span className="rounded-full bg-amber-50 px-2.5 py-0.5 text-xs font-medium text-amber-700 border border-amber-200 shadow-sm">
                      License: {ds.license.replace(/^spdx:/i, '')}
                    </span>
                  )}
                  {ds.url && (
                    <a
                      href={ds.url}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="inline-flex items-center gap-1 text-blue-600 hover:text-blue-500 font-medium"
                    >
                      View on {ds.source} ↗
                    </a>
                  )}
                </div>

                {/* Modality chips, then the reuse summary the sidebar card details */}
                {(modalities.length > 0 || showReuseSummary) && (
                  <div className="mt-4 flex flex-wrap items-center gap-1.5">
                    {modalities.map((m) => (
                      <ModalityChip key={m} label={m} />
                    ))}
                    {trackedMetrics && showReuseSummary && (
                      <span className={`text-sm text-slate-600 ${modalities.length > 0 ? 'ml-2' : ''}`}>
                        <strong className="font-semibold text-emerald-700">
                          {plural(trackedMetrics.reuse_count, 'reuse', 'reuses')}
                        </strong>
                        {' · '}
                        {plural(trackedMetrics.mention_count, 'mention', 'mentions')}
                      </span>
                    )}
                  </div>
                )}
              </div>
            </div>

            {/* Authors */}
            {ds.authors && ds.authors.length > 0 && (
              <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-6 mb-6">
                <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">Authors</h2>
                <div className="flex flex-wrap gap-2">
                  {ds.authors.map((name, i) => (
                    <span
                      key={`author-${i}`}
                      className="inline-flex items-center gap-2 rounded-full bg-white/80 border border-slate-200 px-3 py-1 text-xs shadow-sm"
                    >
                      <span
                        className="w-4 h-4 rounded-full shrink-0 opacity-75"
                        style={{
                          background: `linear-gradient(135deg, ${AUTHOR_COLORS[i % AUTHOR_COLORS.length][0]}, ${AUTHOR_COLORS[i % AUTHOR_COLORS.length][1]})`,
                        }}
                      />
                      <span className="font-medium text-slate-700">{name}</span>
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Contributors */}
            {ds.contributors && ds.contributors.length > 0 && (
              <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-6 mb-6">
                <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">Contributors</h2>
                <div className="flex flex-wrap gap-2">
                  {ds.contributors.map((c: DatasetContributor, i: number) => (
                    <span
                      key={`${c.name}-${i}`}
                      className="inline-flex items-center gap-1.5 rounded-full bg-white/80 border border-slate-200 px-3 py-1 text-xs shadow-sm"
                    >
                      <span className="font-medium text-slate-700">{c.name}</span>
                      {c.roles?.length > 0 && (
                        <span className="text-slate-400">
                          {c.roles.map((r) => r.replace('dcite:', '')).join(', ')}
                        </span>
                      )}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Description / Abstract */}
            {(ds.full_description || ds.description) && (
              <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-6 mb-6">
                <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-3">Description</h2>
                <div className="prose prose-sm prose-slate max-w-none">
                  <ReactMarkdown>{ds.full_description || ds.description || ''}</ReactMarkdown>
                </div>
              </div>
            )}

          </div>

          {/* ─── Right column: dataset impact, then every paper with its label ─── */}
          <div className="w-full lg:w-[460px] flex-shrink-0 space-y-5">
            <DatasetImpactCard metrics={trackedMetrics} loading={!metrics && !metricsError} error={metricsError} />
            <DatasetPaperList primaryPapers={data.primary_papers} citations={data.citations} metrics={trackedMetrics} />
          </div>
        </div>
      </div>
    </div>
  );
}
