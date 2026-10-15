import React, { useEffect, useRef, useState } from 'react';
import type { DatasetMetricsYear, TrackedDatasetMetrics } from '../services/api';
import { LABEL_FILL } from '../utils/classification';
import { doiUrl } from '../utils/doi';
import {
  coverageSummary,
  formatMonthYear,
  independenceSegments,
  paperByline,
  plural,
  splitBarColumns,
  splitBarSummary,
} from '../utils/reuseMetrics';
import { Card, CardTitle, Note, StatGrid, StatTile, SubTitle, cx } from './ui';

/** Height (px) of the busiest year's bar; the plot area leaves room above it. */
const BAR_MAX_PX = 80;
/** Past this many years the empty current year no longer says "none yet". */
const NONE_YET_UP_TO = 8;

function Legend() {
  return (
    <span className="flex items-center gap-3.5 text-[13px] text-ink">
      <span className="flex items-center gap-1.5">
        <span className={cx('h-2.5 w-3.5', LABEL_FILL.REUSE)} aria-hidden="true" />
        Reuse
      </span>
      <span className="flex items-center gap-1.5">
        <span className={cx('h-2.5 w-3.5', LABEL_FILL.MENTION)} aria-hidden="true" />
        Mention
      </span>
    </span>
  );
}

/**
 * Citing papers per year as stacked columns: mentions (orange) over reuse
 * (blue), each segment labelled with its count when tall enough, a y-axis
 * with three ticks and one gridline at the midpoint.
 */
export function PerYearChart({ perYear }: { perYear: DatasetMetricsYear[] }) {
  const columns = splitBarColumns(perYear, BAR_MAX_PX);
  const grid = { gridTemplateColumns: `repeat(${columns.length}, minmax(0, 1fr))` };
  const lastIndex = columns.length - 1;
  const nothingYet = columns.every((col) => col.reuse + col.mentions === 0);
  const maxCount = Math.max(1, ...perYear.map((y) => y.reuse + y.mentions));
  const topTick = maxCount % 2 === 0 ? maxCount : maxCount + 1;
  const plotHeight = (BAR_MAX_PX * topTick) / maxCount;

  if (columns.length === 0) {
    return <p className="py-6 text-center text-xs text-muted">No dated papers yet.</p>;
  }
  return (
    <div
      role="img"
      aria-label={`Citing papers per year. ${splitBarSummary(perYear)}.`}
      className="grid gap-2"
      style={{ gridTemplateColumns: '28px minmax(0, 1fr)' }}
    >
      <div
        aria-hidden="true"
        className="flex flex-col items-end justify-between text-xs leading-none text-muted"
        style={{ height: plotHeight + 4 }}
      >
        <span>{topTick}</span>
        <span>{topTick / 2}</span>
        <span>0</span>
      </div>
      <div className="flex flex-col gap-1.5">
        <div
          className="relative grid items-end border-b border-ink-3"
          style={{
            ...grid,
            height: plotHeight,
            marginTop: 4,
            backgroundImage: 'linear-gradient(rgb(var(--grid)), rgb(var(--grid)))',
            backgroundSize: '100% 1px',
            backgroundPosition: `0 ${plotHeight / 2}px`,
            backgroundRepeat: 'no-repeat',
          }}
        >
          {nothingYet && (
            <span className="absolute inset-0 flex items-center justify-center text-xs text-muted">
              No reuse or mentions yet
            </span>
          )}
          {!nothingYet &&
            columns.map((col, i) => (
              <div key={col.year} className="flex h-full items-end justify-center">
                {col.reuse + col.mentions === 0 ? (
                  i === lastIndex && columns.length <= NONE_YET_UP_TO ? (
                    <span className="whitespace-nowrap pb-1.5 text-xs text-muted">none yet</span>
                  ) : null
                ) : (
                  <div className="flex w-10 max-w-full flex-col gap-[2px]">
                    {col.mentions > 0 && (
                      <div
                        title={`${col.year}: ${plural(col.mentions, 'mention', 'mentions')}`}
                        className={cx('flex items-center justify-center text-[13px] font-bold tabular-nums text-navy', LABEL_FILL.MENTION)}
                        style={{ height: col.mentionHeight }}
                      >
                        {col.showMentionCount ? col.mentions : null}
                      </div>
                    )}
                    {col.reuse > 0 && (
                      <div
                        title={`${col.year}: ${plural(col.reuse, 'reuse', 'reuses')}`}
                        className={cx('flex items-center justify-center text-[13px] font-bold tabular-nums text-white', LABEL_FILL.REUSE)}
                        style={{ height: col.reuseHeight }}
                      >
                        {col.showReuseCount ? col.reuse : null}
                      </div>
                    )}
                  </div>
                )}
              </div>
            ))}
        </div>
        <div aria-hidden="true" className="grid text-center text-[13px] tabular-nums text-ink-2" style={grid}>
          {columns.map((col) => (
            <span key={col.year}>{col.label}</span>
          ))}
        </div>
      </div>
    </div>
  );
}

const HOW_WE_COUNT_ID = 'dataset-how-we-count';

/** Grace period (ms) before closing once the pointer leaves, so it can cross to the popover. */
const HOW_WE_COUNT_CLOSE_MS = 150;

/**
 * "How we count" with an ⓘ bubble: the bubble (not the words) opens the
 * definitions on hover, click or tap (Enter from the keyboard); they close
 * when the pointer leaves, on a click outside, Escape, or focus moving
 * elsewhere.
 */
function HowWeCount() {
  const [open, setOpen] = useState(false);
  const wrap = useRef<HTMLDivElement>(null);
  const button = useRef<HTMLButtonElement>(null);
  const closeTimer = useRef<number | null>(null);

  const cancelClose = () => {
    if (closeTimer.current !== null) window.clearTimeout(closeTimer.current);
    closeTimer.current = null;
  };
  const closeSoon = () => {
    cancelClose();
    closeTimer.current = window.setTimeout(() => setOpen(false), HOW_WE_COUNT_CLOSE_MS);
  };
  useEffect(() => () => {
    if (closeTimer.current !== null) window.clearTimeout(closeTimer.current);
  }, []);

  useEffect(() => {
    if (!open) return undefined;
    const onPointerDown = (e: PointerEvent) => {
      if (e.target instanceof Node && wrap.current?.contains(e.target)) return;
      setOpen(false);
    };
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return;
      setOpen(false);
      button.current?.focus();
    };
    document.addEventListener('pointerdown', onPointerDown);
    window.addEventListener('keydown', onKey);
    return () => {
      document.removeEventListener('pointerdown', onPointerDown);
      window.removeEventListener('keydown', onKey);
    };
  }, [open]);

  return (
    <div className="flex items-center gap-0.5">
      <span className="text-[13.5px] text-muted">How we count</span>
      <div
        ref={wrap}
        className="relative"
        onMouseEnter={() => {
          cancelClose();
          setOpen(true);
        }}
        onMouseLeave={closeSoon}
        onBlur={(e) => {
          if (!wrap.current?.contains(e.relatedTarget as Node | null)) setOpen(false);
        }}
      >
        <button
          ref={button}
          type="button"
          onClick={() => setOpen(true)}
          aria-label="How we count"
          aria-expanded={open}
          aria-controls={HOW_WE_COUNT_ID}
          className="flex h-7 w-7 items-center justify-center rounded-full text-muted transition-colors hover:bg-panel hover:text-link"
        >
          <svg viewBox="0 0 20 20" fill="currentColor" className="h-4 w-4" aria-hidden="true">
            <path
              fillRule="evenodd"
              d="M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0Zm-7-4a1 1 0 1 1-2 0 1 1 0 0 1 2 0ZM9 9a.75.75 0 0 0 0 1.5h.253a.25.25 0 0 1 .244.304l-.459 2.066A1.75 1.75 0 0 0 10.747 15H11a.75.75 0 0 0 0-1.5h-.253a.25.25 0 0 1-.244-.304l.459-2.066A1.75 1.75 0 0 0 9.253 9H9Z"
              clipRule="evenodd"
            />
          </svg>
        </button>
        {open && (
          // Focusable so a click inside it keeps focus within the popover.
          <div
            id={HOW_WE_COUNT_ID}
            role="region"
            aria-label="How we count"
            tabIndex={-1}
            className="absolute right-0 top-full z-20 mt-2 w-[min(340px,calc(100vw-4rem))] rounded-md border border-line bg-surface p-4 text-left shadow-pop outline-none"
          >
            <dl className="m-0 flex flex-col gap-2 text-xs leading-snug text-ink-2">
              <div>
                <dt className="font-bold text-ink">Reuse</dt>
                <dd className="m-0">The citing paper's full text shows it used this dataset's data.</dd>
              </div>
              <div>
                <dt className="font-bold text-ink">Independent</dt>
                <dd className="m-0">
                  None of its authors are authors of the dataset or its papers, and the classifier didn't judge it the
                  dataset's own lab.
                </dd>
              </div>
              <div>
                <dt className="font-bold text-ink">Mention</dt>
                <dd className="m-0">Cites the dataset or its paper without using the data.</dd>
              </div>
              <div>
                <dt className="font-bold text-ink">Per year</dt>
                <dd className="m-0">By the citing paper's publication year, from the dataset's release to now.</dd>
              </div>
              <div>
                <dt className="font-bold text-ink">Coverage</dt>
                <dd className="m-0">
                  Papers are labelled from their full text. Citing papers without full text, or not reached yet, aren't
                  counted until they are.
                </dd>
              </div>
            </dl>
          </div>
        )}
      </div>
    </div>
  );
}

function ImpactContent({ metrics }: { metrics: TrackedDatasetMetrics }) {
  const reuse = metrics.reuse_count;
  const last = metrics.last_reuse;
  const coverage = coverageSummary(metrics.coverage);
  const undated = metrics.undated.reuse + metrics.undated.mentions;
  const segments = independenceSegments(metrics.independent_reuse_count, metrics.same_lab_reuse_count);
  const byline = last ? paperByline(last.first_author, last.author_count) : null;
  const published =
    metrics.published_precision === 'year' ? metrics.published?.slice(0, 4) : formatMonthYear(metrics.published);
  const lastDate = last ? formatMonthYear(last.first_date ?? last.publication_date) : null;

  return (
    <>
      <SubTitle letter="A">Reuse Metrics</SubTitle>
      <StatGrid columns={2}>
        <StatTile first label="Reuse" value={reuse} valueClassName={reuse > 0 ? 'text-link' : 'text-muted'}>
          <span className="text-xs leading-snug text-muted">{reuse === 1 ? 'paper used the data' : 'papers used the data'}</span>
        </StatTile>
        <StatTile
          label="Independent"
          value={
            reuse > 0 ? (
              <span className="flex items-baseline gap-1.5">
                {metrics.independent_reuse_count}
                <span className="text-[13px] font-normal text-muted">of {reuse}</span>
              </span>
            ) : (
              <span className="text-muted">–</span>
            )
          }
        >
          <span
            aria-hidden="true"
            className="mt-1 flex gap-[2px]"
            title={
              reuse > 0
                ? `${plural(metrics.independent_reuse_count, 'independent reuse', 'independent reuses')}, ${metrics.same_lab_reuse_count} by the dataset's lab`
                : undefined
            }
          >
            {segments.length ? (
              segments.map((s, i) => (
                <span key={`${s.kind}-${i}`} className={cx('h-1.5', s.kind === 'independent' ? 'bg-reuse' : 'bg-reuse-soft')} style={{ flexGrow: s.weight }} />
              ))
            ) : (
              <span className="h-1.5 flex-grow bg-line-2" />
            )}
          </span>
          <span className="text-xs text-muted">different lab</span>
          {reuse > 0 && <span className="sr-only">{metrics.same_lab_reuse_count} by the dataset's lab.</span>}
        </StatTile>
        <StatTile first row2 label="Mentions" value={metrics.mention_count.toLocaleString('en-US')}>
          <span className="text-xs leading-snug text-muted">cited without reuse</span>
        </StatTile>
        <StatTile
          row2
          label="Last reused"
          value={last ? lastDate ?? 'Date unknown' : 'Not yet'}
          valueClassName={cx('!text-[22px] !leading-[1.45]', last && lastDate ? '' : 'text-muted')}
        >
          <span className="truncate text-[12.5px]">
            {last ? (
              <a href={doiUrl(last.doi)} target="_blank" rel="noopener noreferrer" title={last.title ?? undefined}>
                {byline ?? 'View paper'}
              </a>
            ) : published ? (
              <span className="text-muted">published {published}</span>
            ) : null}
          </span>
        </StatTile>
      </StatGrid>

      <SubTitle letter="B" aside={<Legend />}>
        Citing Papers per Year
      </SubTitle>
      <PerYearChart perYear={metrics.per_year} />
      <Note className={coverage.incomplete ? 'text-warn-ink' : undefined}>
        {coverage.text}
        {undated > 0 && ` Not in the chart: ${plural(undated, 'undated paper', 'undated papers')}.`}
        {' '}A paper counts once, by publication year.
      </Note>
    </>
  );
}

/**
 * The dataset page's "Dataset Impact" card: reuse, independent reuse,
 * mentions and last reuse as stat tiles, a per-year split column chart and
 * how many citing papers the numbers rest on.
 */
export function DatasetImpactCard({
  metrics,
  loading = false,
  error = null,
  number = 1,
  untrackedNote,
}: {
  metrics: TrackedDatasetMetrics | null;
  loading?: boolean;
  error?: string | null;
  number?: number;
  /** Shown instead of the metrics when the archive has no paper mapping. */
  untrackedNote?: string;
}) {
  const title = (
    <CardTitle number={number} id="impact-h" aside={metrics ? <HowWeCount /> : undefined}>
      Dataset Impact
    </CardTitle>
  );
  if (metrics) {
    return (
      <Card aria-labelledby="impact-h">
        {title}
        <ImpactContent metrics={metrics} />
      </Card>
    );
  }
  if (error) {
    return (
      <Card aria-labelledby="impact-h">
        {title}
        <p className="m-0 text-sm text-ink-2">Impact metrics couldn't be loaded.</p>
        <p className="m-0 text-xs text-muted">{error}</p>
      </Card>
    );
  }
  if (!loading) {
    return (
      <Card aria-labelledby="impact-h">
        {title}
        <p className="m-0 text-sm text-ink-2">{untrackedNote ?? 'No impact metrics for this dataset yet.'}</p>
      </Card>
    );
  }
  return (
    <Card aria-labelledby="impact-h" aria-busy="true">
      {title}
      <SubTitle letter="A">Reuse Metrics</SubTitle>
      <StatGrid columns={2}>
        <StatTile first label="Reuse" loading />
        <StatTile label="Independent" loading />
        <StatTile first row2 label="Mentions" loading />
        <StatTile row2 label="Last reused" loading />
      </StatGrid>
      <SubTitle letter="B">Citing Papers per Year</SubTitle>
      <div className="h-[120px] animate-skeleton rounded bg-panel motion-reduce:animate-none" />
      <div className="h-3 w-3/4 animate-skeleton rounded bg-line-2 motion-reduce:animate-none" />
    </Card>
  );
}
