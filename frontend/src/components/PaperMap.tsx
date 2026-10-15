import React from 'react';
import { LABEL_FILL } from '../utils/classification';
import { histogram, rangeLabel, roleShares, yearOf } from '../utils/datasetCard';
import { PAPER_LABEL_TEXT } from '../utils/datasetPapers';
import type { DatasetPaperItem, PaperLabel } from '../utils/datasetPapers';
import { doiUrl } from '../utils/doi';
import { plural } from '../utils/reuseMetrics';
import { Card, CardTitle, Note, SubTitle, Tag, cx } from './ui';
import type { TagTone } from './ui';

/** How many papers the map shows; the Papers tab has them all. */
export const PAPER_MAP_ROWS = 7;

const ROLE_TONE: Record<PaperLabel, TagTone> = { REUSE: 'reuse', PRIMARY: 'primary', MENTION: 'mention' };

const GRID = 'minmax(0, 1fr) 60px 90px 92px';

/** A six-bar sketch of a column's distribution, as in the poster's table header. */
function MiniHistogram({ counts }: { counts: number[] }) {
  const max = Math.max(1, ...counts);
  return (
    <span aria-hidden="true" className="flex h-6 items-end gap-[3px]">
      {counts.map((c, i) => (
        <span
          key={i}
          className={cx('flex-grow', c > 0 ? 'bg-bar-hist' : 'bg-line')}
          style={{ height: c > 0 ? Math.max(2, Math.round((24 * c) / max)) : 2 }}
        />
      ))}
    </span>
  );
}

function ColumnHeader({
  name,
  type,
  summary,
  children,
  className,
}: {
  name: string;
  type?: string;
  summary: string;
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <div role="columnheader" className={cx('flex flex-col gap-[5px] border-r border-line-inner p-2.5 last:border-r-0', className)}>
      <span className="flex justify-between gap-1.5">
        <span className="font-mono text-[12.5px] font-medium text-ink">{name}</span>
        {type && <span className="font-mono text-[10.5px] text-muted">{type}</span>}
      </span>
      {children}
      <span className="text-[11px] text-muted">{summary}</span>
    </div>
  );
}

/**
 * Section 1 of the dataset card: the papers tied to the dataset as a compact
 * table, with a mini distribution under each column header. Reuse papers
 * come first, then the dataset's own papers, then mentions, newest first.
 */
export function PaperMap({
  papers,
  sameLabByDoi,
  totalLinked,
  onShowAll,
}: {
  papers: DatasetPaperItem[];
  sameLabByDoi: Map<string, boolean>;
  /** The full count, when more than the rows shown. */
  totalLinked: number;
  onShowAll: () => void;
}) {
  const shown = papers.slice(0, PAPER_MAP_ROWS);
  const titleLengths = papers.map((p) => (p.title ?? p.doi).length);
  const years = papers.map((p) => yearOf(p.date)).filter((y): y is number => y !== null);
  const roles = roleShares(papers);
  const labCounts = papers.reduce(
    (acc, p) => {
      if (p.label !== 'REUSE') return acc;
      const same = sameLabByDoi.get(p.doi) ?? p.citation?.same_lab === true;
      if (same) acc.same += 1;
      else acc.independent += 1;
      return acc;
    },
    { independent: 0, same: 0 },
  );
  const labTotal = labCounts.independent + labCounts.same;
  const reuseCount = papers.filter((p) => p.label === 'REUSE').length;

  return (
    <Card aria-labelledby="map-h">
      <CardTitle number={1} id="map-h" aside={totalLinked > shown.length ? <button type="button" onClick={onShowAll} className="text-link hover:underline">All {totalLinked} papers</button> : undefined}>
        Paper Map
      </CardTitle>
      <SubTitle letter="A">Linked Papers</SubTitle>
      {papers.length === 0 ? (
        <p className="m-0 text-sm italic text-muted">No papers have been mapped to this dataset yet.</p>
      ) : (
        <div role="table" aria-label="Linked papers" className="flex flex-col overflow-hidden rounded-[5px] border border-line-2">
          <div role="row" className="grid border-b border-line-2 bg-panel-head" style={{ gridTemplateColumns: GRID }}>
            <ColumnHeader name="title" type="string" summary={rangeLabel(titleLengths, 'characters')} className="px-3">
              <MiniHistogram counts={histogram(titleLengths, 4)} />
            </ColumnHeader>
            <ColumnHeader name="year" summary={years.length ? rangeLabel(years) : 'undated'}>
              <MiniHistogram counts={histogram(years, 6)} />
            </ColumnHeader>
            <ColumnHeader name="role" type="class" summary={plural(roles.length, 'class', 'classes')}>
              <span aria-hidden="true" className="flex h-6 items-end">
                <span className="flex h-[9px] w-full gap-[2px]">
                  {roles.map((r) => (
                    <span key={r.label} className={LABEL_FILL[r.label]} style={{ width: `${r.share * 100}%` }} title={`${PAPER_LABEL_TEXT[r.label]}: ${r.count}`} />
                  ))}
                </span>
              </span>
            </ColumnHeader>
            <ColumnHeader name="lab" type="class" summary={labTotal ? `${labCounts.independent} independent` : reuseCount ? 'unknown' : 'no reuse'}>
              <span aria-hidden="true" className="flex h-6 items-end">
                <span className="flex h-[9px] w-full gap-[2px]">
                  {labTotal > 0 ? (
                    <>
                      <span className="bg-reuse" style={{ width: `${(100 * labCounts.independent) / labTotal}%` }} />
                      <span className="bg-reuse-soft" style={{ width: `${(100 * labCounts.same) / labTotal}%` }} />
                    </>
                  ) : (
                    <span className="w-full bg-line" />
                  )}
                </span>
              </span>
            </ColumnHeader>
          </div>
          {shown.map((p) => {
            const year = yearOf(p.date);
            const same = p.label === 'REUSE' ? sameLabByDoi.get(p.doi) ?? (p.citation?.same_lab === true ? true : undefined) : undefined;
            return (
              <div key={p.doi} role="row" className="grid border-t border-line-3 first:border-t-0" style={{ gridTemplateColumns: GRID }}>
                <span role="cell" className="flex min-w-0 flex-col gap-0.5 border-r border-line-3 px-3 py-[9px]">
                  <a
                    href={doiUrl(p.doi)}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="line-clamp-2 text-[13.5px] leading-[1.4] text-ink hover:text-link"
                    title={p.title ?? p.doi}
                  >
                    {p.title || p.doi}
                  </a>
                  <span className="truncate text-xs italic text-muted">
                    {p.journal ?? (p.isPreprint ? 'Preprint' : 'Journal unknown')}
                  </span>
                </span>
                <span role="cell" className="border-r border-line-3 px-2.5 py-[9px] font-mono text-[12.5px] text-ink">
                  {year ?? '—'}
                </span>
                <span role="cell" className="border-r border-line-3 px-2.5 py-2">
                  <Tag tone={ROLE_TONE[p.label]}>{PAPER_LABEL_TEXT[p.label]}</Tag>
                </span>
                <span role="cell" className="px-2.5 py-2 text-xs text-ink-3">
                  {same === true && <Tag tone="neutral" title="Shares an author with the dataset or its papers, or the classifier judged it the dataset's own lab">Same lab</Tag>}
                  {same === false && <Tag tone="reuse" title="No authors in common with the dataset or its papers">Independent</Tag>}
                  {same === undefined && p.label === 'REUSE' && <span className="text-muted">—</span>}
                </span>
              </div>
            );
          })}
        </div>
      )}
      {papers.length > 0 && (
        <Note>
          {totalLinked > shown.length
            ? `${totalLinked} papers are linked; the ${shown.length} that matter most are shown, reuse first. `
            : `${plural(papers.length, 'paper is', 'papers are')} linked. `}
          Roles are assigned from each citing paper's full text. Lab is whether the paper shares an author with the dataset.
        </Note>
      )}
    </Card>
  );
}
