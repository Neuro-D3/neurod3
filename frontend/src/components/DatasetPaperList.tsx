import React, { useMemo, useState } from 'react';
import type { DatasetDetailCitation, DatasetDetailPaper, TrackedDatasetMetrics } from '../services/api';
import { confidenceShort, modalityLabel, primaryQuote, reuseTypeLabel, reusedModalities } from '../utils/classification';
import {
  PAPER_LABELS,
  PAPER_LABEL_HELP,
  PAPER_LABEL_TEXT,
  buildPaperList,
  countByLabel,
} from '../utils/datasetPapers';
import type { DatasetPaperItem, PaperLabel } from '../utils/datasetPapers';
import { doiUrl } from '../utils/doi';
import { formatMonthYear, plural } from '../utils/reuseMetrics';

// Same colours as the Dataset impact card: reuse blue, mention orange.
const LABEL_BADGE: Record<PaperLabel, string> = {
  PRIMARY: 'bg-slate-100 text-slate-700 border-slate-300',
  REUSE: 'bg-blue-700 text-white border-blue-700',
  MENTION: 'bg-orange-100 text-orange-900 border-orange-300',
};
const LABEL_DOT: Record<PaperLabel, string> = {
  PRIMARY: 'bg-slate-400',
  REUSE: 'bg-blue-700',
  MENTION: 'bg-orange-300',
};

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

function formatAuthors(authors: string[], max = 3): string {
  if (!authors.length) return 'Unknown authors';
  if (authors.length <= max) return authors.join(', ');
  return `${authors.slice(0, max).join(', ')} et al.`;
}

function Pill({ className, title, children }: { className: string; title?: string; children: React.ReactNode }) {
  return (
    <span className={`inline-block rounded-full border px-2 py-0.5 text-[10px] font-semibold ${className}`} title={title}>
      {children}
    </span>
  );
}

function Evidence({ c }: { c: DatasetDetailCitation }) {
  const quote = primaryQuote(c.evidence_quotes);
  const hallucinated = c.hallucinated_quote_count ?? 0;
  return (
    <div className="mt-2 space-y-1.5 rounded-md bg-slate-50 p-2.5">
      {quote && (
        <blockquote
          className="border-l-2 border-slate-300 pl-2 text-[11px] leading-relaxed text-slate-700"
          title={quote.match_type ? `Quote verified against the paper text (${quote.match_type})` : undefined}
        >
          “{quote.quote}”
        </blockquote>
      )}
      {c.reasoning && <p className="text-[11px] italic leading-relaxed text-slate-600">{c.reasoning}</p>}
      {hallucinated > 0 && (
        <p className="text-[10px] text-rose-600">
          {plural(hallucinated, 'quote', 'quotes')} could not be matched word for word
        </p>
      )}
      {typeof c.confidence === 'number' && (
        <p className="text-[10px] text-slate-500">Confidence: {confidenceShort(c.confidence).text}</p>
      )}
    </div>
  );
}

function PaperRow({ paper, sameLab }: { paper: DatasetPaperItem; sameLab?: boolean }) {
  const [showEvidence, setShowEvidence] = useState(false);
  const c = paper.citation;
  const isReuse = paper.label === 'REUSE';
  const typeLabel = isReuse && c ? reuseTypeLabel(c.reuse_type, c.reuse_type_other) : null;
  const modalities = isReuse && c ? reusedModalities(c.reused_modalities) : [];
  // The metrics' same-lab call (classifier or shared author names) when loaded,
  // so the tag agrees with the Independent count; else the classifier's.
  const lab = isReuse ? sameLab ?? (c?.same_lab === true ? true : undefined) : undefined;
  const hasEvidence = Boolean(c && (primaryQuote(c.evidence_quotes) || c.reasoning));
  const country = countryLabel(paper.country);
  const date = formatMonthYear(paper.date);

  return (
    <article className="rounded-lg border border-slate-200 bg-white p-3">
      <div className="flex flex-wrap items-center gap-1.5">
        <Pill className={LABEL_BADGE[paper.label]} title={PAPER_LABEL_HELP[paper.label]}>
          {PAPER_LABEL_TEXT[paper.label]}
        </Pill>
        {typeLabel && <Pill className="border-violet-200 bg-violet-50 uppercase tracking-wide text-violet-700">{typeLabel}</Pill>}
        {lab === true && (
          <Pill
            className="border-amber-200 bg-amber-50 text-amber-800"
            title="Shares an author with the dataset or its papers, or the classifier judged it the dataset's own lab"
          >
            Same lab
          </Pill>
        )}
        {lab === false && (
          <Pill className="border-blue-200 bg-blue-50 text-blue-800" title="No authors in common with the dataset or its papers">
            Independent
          </Pill>
        )}
        {date && <span className="ml-auto text-[11px] tabular-nums text-slate-500">{date}</span>}
      </div>

      <a
        href={doiUrl(paper.doi)}
        target="_blank"
        rel="noopener noreferrer"
        className="mt-1.5 block text-[13px] font-semibold leading-snug text-blue-700 line-clamp-2 hover:text-blue-900 hover:underline"
      >
        {paper.title || paper.doi}
      </a>
      <p className="mt-0.5 text-[11px] text-slate-500">
        {formatAuthors(paper.authors)}
        {paper.journal && <> &middot; <em>{paper.journal}</em></>}
        {country && <> &middot; {country}</>}
      </p>

      {(modalities.length > 0 || (isReuse && c?.source_archive)) && (
        <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
          {modalities.map((m) => (
            <span key={m} className="rounded-full border border-blue-100 bg-blue-50/80 px-2 py-0.5 text-[10px] font-medium text-blue-700">
              {modalityLabel(m)}
            </span>
          ))}
          {c?.source_archive && (
            <span className="text-[10px] text-slate-500" title="Where the paper says it obtained the data">
              via {c.source_archive}
            </span>
          )}
        </div>
      )}

      {paper.label === 'PRIMARY' && paper.citingCount !== undefined && (
        <p className="mt-1 text-[11px] text-slate-500">
          {plural(paper.citingCount, 'citing paper', 'citing papers')} found
          {paper.openalexId && (
            <>
              {' '}
              &middot;{' '}
              <a
                href={paper.openalexId.startsWith('http') ? paper.openalexId : `https://openalex.org/${paper.openalexId}`}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-700 hover:underline"
              >
                OpenAlex
              </a>
            </>
          )}
        </p>
      )}

      {hasEvidence && c && (
        <>
          <button
            type="button"
            onClick={() => setShowEvidence((v) => !v)}
            aria-expanded={showEvidence}
            className="mt-1.5 text-[11px] font-medium text-slate-600 hover:text-slate-900"
          >
            {showEvidence ? 'Hide evidence ▴' : 'Evidence ▾'}
          </button>
          {showEvidence && <Evidence c={c} />}
        </>
      )}
    </article>
  );
}

/**
 * Every paper tied to the dataset in one list, each with its label: the
 * dataset's primary papers and the citing papers labelled Reuse or Mention.
 * Chips filter by label.
 */
export function DatasetPaperList({
  primaryPapers,
  citations,
  metrics,
}: {
  primaryPapers: DatasetDetailPaper[];
  citations: DatasetDetailCitation[];
  metrics: TrackedDatasetMetrics | null;
}) {
  const items = useMemo(() => buildPaperList(primaryPapers, citations), [primaryPapers, citations]);
  const counts = countByLabel(items);
  const [filter, setFilter] = useState<PaperLabel | 'ALL'>('ALL');
  const shown = filter === 'ALL' ? items : items.filter((p) => p.label === filter);
  const sameLabByDoi = useMemo(
    () => new Map((metrics?.reuse_papers ?? []).map((p) => [p.doi, p.same_lab])),
    [metrics],
  );
  // The detail endpoint returns at most 250 citations; say so when the metrics count more.
  const totals: Partial<Record<PaperLabel, number>> = metrics
    ? { REUSE: metrics.reuse_count, MENTION: metrics.mention_count }
    : {};
  const cut = (['REUSE', 'MENTION'] as const).filter(
    (label) => (filter === 'ALL' || filter === label) && (totals[label] ?? 0) > counts[label],
  );

  return (
    <section aria-labelledby="dataset-papers-title" className="rounded-2xl border border-white/20 bg-white/70 p-5 shadow-xl backdrop-blur-xl">
      <h2 id="dataset-papers-title" className="text-xs font-semibold uppercase tracking-wider text-slate-500">
        Papers <span className="ml-1 font-normal text-slate-400">({items.length})</span>
      </h2>

      {items.length === 0 ? (
        <p className="mt-3 text-sm italic text-slate-500">No papers have been mapped to this dataset yet.</p>
      ) : (
        <>
          <div role="group" aria-label="Show papers by label" className="mt-3 flex flex-wrap gap-1.5">
            {(['ALL', ...PAPER_LABELS] as const).map((key) => {
              const active = filter === key;
              const count = key === 'ALL' ? items.length : counts[key];
              return (
                <button
                  key={key}
                  type="button"
                  aria-pressed={active}
                  disabled={count === 0}
                  onClick={() => setFilter(key)}
                  className={`inline-flex items-center gap-1.5 rounded-full border px-3 py-1.5 text-xs font-medium transition-colors disabled:cursor-default disabled:opacity-40 ${
                    active
                      ? 'border-slate-900 bg-slate-900 text-white'
                      : 'border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
                  }`}
                >
                  {key !== 'ALL' && <span className={`h-2 w-2 rounded-full ${LABEL_DOT[key]}`} aria-hidden="true" />}
                  {key === 'ALL' ? 'All' : PAPER_LABEL_TEXT[key]}
                  <span className={`tabular-nums ${active ? 'text-slate-300' : 'text-slate-500'}`}>{count}</span>
                </button>
              );
            })}
          </div>

          <ul className="mt-3 max-h-[70vh] space-y-2.5 overflow-y-auto pr-1">
            {shown.map((paper) => (
              <li key={paper.doi}>
                <PaperRow paper={paper} sameLab={sameLabByDoi.get(paper.doi)} />
              </li>
            ))}
          </ul>

          {cut.map((label) => (
            <p key={label} className="mt-2 text-[11px] text-slate-500">
              Showing {counts[label].toLocaleString('en-US')} of{' '}
              {plural(totals[label] ?? 0, label === 'REUSE' ? 'reuse' : 'mention', label === 'REUSE' ? 'reuses' : 'mentions')}.
            </p>
          ))}
        </>
      )}
    </section>
  );
}
