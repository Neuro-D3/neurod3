import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import ReactMarkdown from 'react-markdown';
import { fetchDatasetDetail } from '../services/api';
import type { DatasetDetailResponse, DatasetDetailPaper, DatasetDetailCitation, DatasetContributor } from '../services/api';
import { PopulationIcon } from '../components/PopulationIcon';
import {
  CONFIDENCE_TIERS,
  confidenceLabel,
  confidenceTier,
  isReuseClassification,
  modalityLabel,
  primaryQuote,
  reuseTypeLabel,
  reusedModalities,
} from '../utils/classification';

const SOURCE_COLORS: Record<string, string> = {
  DANDI: 'bg-purple-100 text-purple-800',
  OpenNeuro: 'bg-green-100 text-green-800',
  Kaggle: 'bg-blue-100 text-blue-800',
  PhysioNet: 'bg-orange-100 text-orange-800',
};

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

function doiUrl(doi: string) {
  return `https://doi.org/${encodeURIComponent(doi).replace(/%2F/gi, '/')}`;
}

const COUNTRY_NAMES: Record<string, string> = {
  US: 'United States', GB: 'United Kingdom', DE: 'Germany', FR: 'France',
  CA: 'Canada', AU: 'Australia', JP: 'Japan', CN: 'China', NL: 'Netherlands',
  CH: 'Switzerland', SE: 'Sweden', IT: 'Italy', ES: 'Spain', KR: 'South Korea',
  BR: 'Brazil', IN: 'India', IL: 'Israel', AT: 'Austria', BE: 'Belgium',
  DK: 'Denmark', NO: 'Norway', FI: 'Finland', SG: 'Singapore', NZ: 'New Zealand',
  IE: 'Ireland', PT: 'Portugal', PL: 'Poland', CZ: 'Czech Republic', HU: 'Hungary',
  TW: 'Taiwan', HK: 'Hong Kong', MX: 'Mexico', AR: 'Argentina', CL: 'Chile',
  ZA: 'South Africa', RU: 'Russia', TR: 'Turkey', GR: 'Greece', RO: 'Romania',
};
function countryLabel(code: string | null | undefined): string | null {
  if (!code) return null;
  return COUNTRY_NAMES[code.toUpperCase()] || code.toUpperCase();
}

function formatAuthors(authors: string[] | null | undefined, max = 5): string {
  if (!authors?.length) return 'Unknown authors';
  if (authors.length <= max) return authors.join(', ');
  return `${authors.slice(0, max).join(', ')} et al.`;
}

const TIER_LABEL_VALUE: Record<string, number> = { high: 9, medium: 5, low: 2 };

function ReuseTypePill({ label }: { label: string }) {
  return (
    <span className="inline-block rounded-full bg-violet-50 border border-violet-200 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-violet-700">
      {label}
    </span>
  );
}

function ReusedModalityChip({ label }: { label: string }) {
  return (
    <span className="inline-block rounded-full bg-blue-50/80 border border-blue-100 px-2 py-0.5 text-[10px] font-medium text-blue-700">
      {modalityLabel(label)}
    </span>
  );
}

function ReusePaperCard({ c, bg }: { c: DatasetDetailCitation; bg: string }) {
  const typeLabel = reuseTypeLabel(c.reuse_type, c.reuse_type_other);
  const modalities = reusedModalities(c.reused_modalities);
  const quote = primaryQuote(c.evidence_quotes);
  const hallucinated = (c.hallucinated_quote_count ?? 0) > 0;
  return (
    <div className={`rounded-lg border p-3 ${bg}`}>
      <a
        href={doiUrl(c.citing_paper_doi)}
        target="_blank"
        rel="noopener noreferrer"
        className="text-xs font-semibold text-blue-700 hover:text-blue-600 hover:underline leading-snug line-clamp-2"
      >
        {c.citing_paper_title || c.citing_paper_doi}
      </a>
      <p className="mt-0.5 text-[11px] text-slate-500">
        {formatAuthors(c.citing_authors, 3)}
        {c.citing_journal && <> &middot; <em>{c.citing_journal}</em></>}
        {c.citing_publication_date && (
          <> &middot; {new Date(c.citing_publication_date).toLocaleDateString()}</>
        )}
        {countryLabel(c.citing_senior_author_country) && (
          <> &middot; {countryLabel(c.citing_senior_author_country)}</>
        )}
      </p>
      {(typeLabel || modalities.length > 0 || c.same_lab != null || c.source_archive) && (
        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
          {typeLabel && <ReuseTypePill label={typeLabel} />}
          {modalities.map((m) => (
            <ReusedModalityChip key={m} label={m} />
          ))}
          {c.same_lab === true && (
            <span
              className="inline-block rounded-full bg-amber-50 border border-amber-200 px-2 py-0.5 text-[10px] font-medium text-amber-700"
              title="The reusing authors overlap with the dataset's contributors"
            >
              Same lab
            </span>
          )}
          {c.source_archive && (
            <span className="text-[10px] text-slate-400" title="Where the paper says it obtained the data">
              via {c.source_archive}
            </span>
          )}
        </div>
      )}
      {quote && (
        <blockquote
          className="mt-2 border-l-2 border-slate-300 pl-2 text-[11px] text-slate-600 leading-relaxed"
          title={quote.match_type ? `Quote verified against the paper text (${quote.match_type})` : undefined}
        >
          “{quote.quote}”
        </blockquote>
      )}
      {c.reasoning && (
        <p className="mt-1 text-[11px] text-slate-500 italic leading-relaxed">{c.reasoning}</p>
      )}
      {hallucinated && (
        <p className="mt-1 text-[10px] text-rose-600">
          {c.hallucinated_quote_count} quote{c.hallucinated_quote_count === 1 ? '' : 's'} could not be matched word for word
        </p>
      )}
      {typeof c.confidence === 'number' && (
        <p className="mt-1 text-[10px] text-slate-400">Confidence {c.confidence}/10</p>
      )}
    </div>
  );
}

function PaperCard({
  paper,
  citations,
  expanded,
  onToggle,
}: {
  paper: DatasetDetailPaper;
  citations: DatasetDetailCitation[];
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="rounded-lg border border-slate-200/60 bg-white/60 backdrop-blur shadow-sm hover:shadow-md transition-shadow">
      <div className="px-3 py-2">
        <a
          href={doiUrl(paper.paper_doi)}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs font-semibold text-blue-700 hover:text-blue-600 hover:underline leading-snug line-clamp-2"
        >
          {paper.paper_title || paper.paper_doi}
        </a>
        <p className="mt-0.5 text-[11px] text-slate-500 line-clamp-1">{formatAuthors(paper.authors)}</p>
        {paper.journal && (
          <p className="mt-0.5 text-[11px] italic text-slate-400 line-clamp-1">{paper.journal}</p>
        )}
        <div className="mt-1 flex flex-wrap items-center gap-2 text-[11px] text-slate-400">
          {paper.publication_date && (
            <span>{new Date(paper.publication_date).toLocaleDateString()}</span>
          )}
          {paper.publication_year && !paper.publication_date && (
            <span>{paper.publication_year}</span>
          )}
          {countryLabel(paper.senior_author_country) && (
            <span>{countryLabel(paper.senior_author_country)}</span>
          )}
          {paper.doi_source && (
            <span className="rounded bg-slate-100/70 px-1 py-px text-[10px] font-medium text-slate-500 border border-slate-200/60">
              {paper.doi_source}
            </span>
          )}
          {paper.openalex_id && (
            <a
              href={paper.openalex_id.startsWith('http') ? paper.openalex_id : `https://openalex.org/${paper.openalex_id}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-blue-500 hover:underline"
            >
              OpenAlex
            </a>
          )}
          {paper.citing_papers_count > 0 && (
            <button
              type="button"
              onClick={onToggle}
              className="ml-auto rounded-full bg-emerald-50/80 px-2 py-0.5 text-[10px] font-semibold text-emerald-700 hover:bg-emerald-100 transition-colors border border-emerald-100"
            >
              {paper.citing_papers_count} citing{expanded ? ' ▴' : ' ▾'}
            </button>
          )}
        </div>
      </div>

      {expanded && citations.length > 0 && (
        <div className="border-t border-slate-100/80 bg-slate-50/40 px-4 py-3">
          <div className="space-y-2">
            {citations.map((c, i) => (
              <div key={`${c.citing_paper_doi}-${i}`} className="text-xs">
                <a
                  href={doiUrl(c.citing_paper_doi)}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="font-medium text-blue-600 hover:underline leading-snug"
                >
                  {c.citing_paper_title || c.citing_paper_doi}
                </a>
                <p className="mt-0.5 text-slate-500">
                  {formatAuthors(c.citing_authors, 3)}
                  {c.citing_journal && <> &middot; <em>{c.citing_journal}</em></>}
                  {c.citing_publication_date && (
                    <> &middot; {new Date(c.citing_publication_date).toLocaleDateString()}</>
                  )}
                  {countryLabel(c.citing_senior_author_country) && (
                    <> &middot; {countryLabel(c.citing_senior_author_country)}</>
                  )}
                </p>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
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
  const [expandedPapers, setExpandedPapers] = useState<Set<string>>(new Set());

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

  const togglePaper = (doi: string) => {
    setExpandedPapers((prev) => {
      const next = new Set(prev);
      if (next.has(doi)) next.delete(doi);
      else next.add(doi);
      return next;
    });
  };

  const citationsByPrimary = (doi: string): DatasetDetailCitation[] =>
    data?.citations.filter((c) => c.primary_paper_doi === doi) ?? [];

  // REUSE from the whole-paper classifier. MENTION / NEITHER / PRIMARY are
  // deliberately not shown here: only papers that actually used the data.
  const reusePapers = (data?.citations ?? []).filter((c) => isReuseClassification(c.classification));

  const reusePapersByTier = (tier: string) =>
    reusePapers.filter((c) => confidenceTier(c.confidence) === tier);

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
                    <span>Published {new Date(ds.created_at).toLocaleDateString()}</span>
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

                {/* Modality chips */}
                {modalities.length > 0 && (
                  <div className="mt-4 flex flex-wrap gap-1.5">
                    {modalities.map((m) => (
                      <ModalityChip key={m} label={m} />
                    ))}
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

          {/* ─── Right column: papers sidebar ─── */}
          <div className="w-full lg:w-[460px] flex-shrink-0 lg:sticky lg:top-24 space-y-5">
            <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-5">
              <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-4">
                Associated Papers
                <span className="ml-2 text-slate-300 font-normal">
                  ({data.primary_papers.length + reusePapers.length})
                </span>
              </h2>
              {data.primary_papers.length === 0 ? (
                <p className="text-sm text-slate-400 italic">No papers have been mapped to this dataset yet.</p>
              ) : (
                <div className="space-y-3 max-h-[calc(100vh-8rem)] overflow-y-auto pr-1">
                  {data.primary_papers.map((paper) => (
                    <PaperCard
                      key={paper.paper_doi}
                      paper={paper}
                      citations={citationsByPrimary(paper.paper_doi)}
                      expanded={expandedPapers.has(paper.paper_doi)}
                      onToggle={() => togglePaper(paper.paper_doi)}
                    />
                  ))}
                </div>
              )}
            </div>

            {/* AI-Identified Reuse Papers */}
            {reusePapers.length > 0 && (
              <div className="rounded-2xl bg-white/70 backdrop-blur-xl border border-white/20 shadow-xl p-5">
                <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-1">
                  AI-Identified Reuse Papers
                  <span className="ml-2 text-slate-300 font-normal">({reusePapers.length})</span>
                </h2>
                <p className="text-[11px] text-slate-400 mb-4">
                  Papers whose full text shows they reused this dataset's data. Each verdict quotes the passage it was judged from.
                </p>
                <div className="space-y-4 max-h-[calc(100vh-8rem)] overflow-y-auto pr-1">
                  {CONFIDENCE_TIERS.map((tier) => {
                    const papers = reusePapersByTier(tier);
                    if (papers.length === 0) return null;
                    const conf = confidenceLabel(TIER_LABEL_VALUE[tier]);
                    return (
                      <div key={tier}>
                        <h3 className={`text-xs font-semibold mb-2 ${conf.color}`}>
                          {conf.text} ({papers.length})
                        </h3>
                        <div className="space-y-2">
                          {papers.map((c, i) => (
                            <ReusePaperCard key={`${c.citing_paper_doi}-${i}`} c={c} bg={conf.bg} />
                          ))}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
