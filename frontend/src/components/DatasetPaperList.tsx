import React, { useMemo, useState } from 'react';
import type { DatasetDetailCitation, TrackedDatasetMetrics } from '../services/api';
import { LABEL_FILL, confidenceShort, modalityLabel, primaryQuote, reuseTypeLabel, reusedModalities } from '../utils/classification';
import { PAPER_LABELS, PAPER_LABEL_HELP, PAPER_LABEL_TEXT, countByLabel } from '../utils/datasetPapers';
import type { DatasetPaperItem, PaperLabel } from '../utils/datasetPapers';
import { doiUrl } from '../utils/doi';
import { formatMonthYear, plural } from '../utils/reuseMetrics';
import { Card, CardTitle, Chip, Tag, cx } from './ui';
import type { TagTone } from './ui';

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

const LABEL_TONE: Record<PaperLabel, TagTone> = { REUSE: 'reuse', PRIMARY: 'primary', MENTION: 'mention' };

function Evidence({ c }: { c: DatasetDetailCitation }) {
  const quote = primaryQuote(c.evidence_quotes);
  const hallucinated = c.hallucinated_quote_count ?? 0;
  return (
    <div className="mt-2 space-y-1.5 rounded-[5px] bg-panel p-3">
      {quote && (
        <blockquote
          className="m-0 border-l-2 border-line-rule pl-2.5 text-xs leading-relaxed text-ink-2"
          title={quote.match_type ? `Quote verified against the paper text (${quote.match_type})` : undefined}
        >
          “{quote.quote}”
        </blockquote>
      )}
      {c.reasoning && <p className="m-0 text-xs italic leading-relaxed text-ink-3">{c.reasoning}</p>}
      {hallucinated > 0 && (
        <p className="m-0 text-[11px] text-danger-ink">
          {plural(hallucinated, 'quote', 'quotes')} could not be matched word for word
        </p>
      )}
      {typeof c.confidence === 'number' && (
        <p className="m-0 text-[11px] text-muted">Confidence: {confidenceShort(c.confidence).text}</p>
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
  // A published paper that was posted as a preprint first: link the preprint.
  const preprint = paper.isPreprint ? undefined : paper.versions.find((v) => v.is_preprint);

  return (
    <article className="border-t border-line-3 py-3 first:border-t-0 first:pt-0">
      <div className="flex flex-wrap items-center gap-1.5">
        <Tag tone={LABEL_TONE[paper.label]} title={PAPER_LABEL_HELP[paper.label]}>
          {PAPER_LABEL_TEXT[paper.label]}
        </Tag>
        {paper.isPreprint && (
          <Tag tone="neutral" title="Not published in a journal yet, as far as we know">
            Preprint
          </Tag>
        )}
        {typeLabel && <Tag tone="tan">{typeLabel}</Tag>}
        {lab === true && (
          <Tag tone="neutral" title="Shares an author with the dataset or its papers, or the classifier judged it the dataset's own lab">
            Same lab
          </Tag>
        )}
        {lab === false && (
          <Tag tone="reuse" title="No authors in common with the dataset or its papers">
            Independent
          </Tag>
        )}
        {date && <span className="ml-auto font-mono text-xs tabular-nums text-muted">{date}</span>}
      </div>

      <a
        href={doiUrl(paper.doi)}
        target="_blank"
        rel="noopener noreferrer"
        className="mt-1.5 line-clamp-2 block text-[14px] font-bold leading-snug text-ink hover:text-link"
      >
        {paper.title || paper.doi}
      </a>
      <p className="m-0 mt-0.5 text-xs text-muted">
        {formatAuthors(paper.authors)}
        {paper.journal && <> &middot; <em>{paper.journal}</em></>}
        {country && <> &middot; {country}</>}
      </p>
      {preprint && (
        <p className="m-0 mt-0.5 text-xs text-muted">
          <a href={doiUrl(preprint.doi)} target="_blank" rel="noopener noreferrer">
            Preprint posted {formatMonthYear(preprint.publication_date) ?? 'earlier'} ↗
          </a>
        </p>
      )}

      {(modalities.length > 0 || (isReuse && c?.source_archive)) && (
        <div className="mt-2 flex flex-wrap items-center gap-1.5">
          {modalities.map((m) => (
            <Chip key={m}>{modalityLabel(m)}</Chip>
          ))}
          {c?.source_archive && (
            <span className="text-[11px] text-muted" title="Where the paper says it obtained the data">
              via {c.source_archive}
            </span>
          )}
        </div>
      )}

      {paper.label === 'PRIMARY' && paper.citingCount !== undefined && (
        <p className="m-0 mt-1 text-xs text-muted">
          {plural(paper.citingCount, 'citing paper', 'citing papers')} found
          {paper.openalexId && (
            <>
              {' '}
              &middot;{' '}
              <a
                href={paper.openalexId.startsWith('http') ? paper.openalexId : `https://openalex.org/${paper.openalexId}`}
                target="_blank"
                rel="noopener noreferrer"
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
            className="mt-1.5 text-xs font-bold text-ink-3 hover:text-ink"
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
  items,
  metrics,
  number,
}: {
  items: DatasetPaperItem[];
  metrics: TrackedDatasetMetrics | null;
  number?: number;
}) {
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
    <Card aria-labelledby="dataset-papers-title">
      <CardTitle number={number} id="dataset-papers-title" aside={<span className="text-muted">{plural(items.length, 'paper', 'papers')}</span>}>
        Papers
      </CardTitle>

      {items.length === 0 ? (
        <p className="m-0 text-sm italic text-muted">No papers have been mapped to this dataset yet.</p>
      ) : (
        <>
          <div role="group" aria-label="Show papers by label" className="flex flex-wrap gap-1.5">
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
                  className={cx(
                    'inline-flex h-8 items-center gap-1.5 rounded-[5px] border px-3 text-[13px] font-bold transition-colors disabled:cursor-default disabled:opacity-40',
                    active ? 'border-navy bg-navy text-white' : 'border-line bg-white text-ink-2 hover:bg-panel',
                  )}
                >
                  {key !== 'ALL' && <span className={cx('h-2.5 w-2.5', LABEL_FILL[key])} aria-hidden="true" />}
                  {key === 'ALL' ? 'All' : PAPER_LABEL_TEXT[key]}
                  <span className={cx('font-normal tabular-nums', active ? 'text-navy-sub' : 'text-muted')}>{count}</span>
                </button>
              );
            })}
          </div>

          <div className="flex flex-col">
            {shown.map((paper) => (
              <PaperRow key={paper.doi} paper={paper} sameLab={sameLabByDoi.get(paper.doi)} />
            ))}
          </div>

          {cut.map((label) => (
            <p key={label} className="m-0 text-xs text-muted">
              Showing {counts[label].toLocaleString('en-US')} of{' '}
              {plural(totals[label] ?? 0, label === 'REUSE' ? 'reuse' : 'mention', label === 'REUSE' ? 'reuses' : 'mentions')}.
            </p>
          ))}
        </>
      )}
    </Card>
  );
}
