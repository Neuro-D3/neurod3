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

/** Height (px) of the busiest year's bar; the plot area leaves room above it. */
const BAR_MAX_PX = 80;
/** Past this many years the empty current year no longer says "none yet". */
const NONE_YET_UP_TO = 8;

const CARD_CLASS =
  'relative flex aspect-square w-full flex-col justify-between rounded-2xl border border-slate-200 bg-white p-[22px] shadow-xl';
const LABEL_CLASS = 'text-[11px] font-semibold uppercase tracking-wider text-slate-500';

function Stat({
  label,
  value,
  valueClassName = 'text-[28px] text-slate-900',
  children,
}: {
  label: string;
  value: React.ReactNode;
  valueClassName?: string;
  children?: React.ReactNode;
}) {
  return (
    <div className="flex min-w-0 flex-col gap-1 rounded-[10px] bg-slate-50 px-3.5 py-3">
      <div className={LABEL_CLASS}>{label}</div>
      <div className={`font-bold leading-tight tabular-nums ${valueClassName}`}>{value}</div>
      {children}
    </div>
  );
}

function PerYearChart({ perYear }: { perYear: DatasetMetricsYear[] }) {
  const columns = splitBarColumns(perYear, BAR_MAX_PX);
  const grid = { gridTemplateColumns: `repeat(${columns.length}, minmax(0, 1fr))` };
  const lastIndex = columns.length - 1;
  const nothingYet = columns.every((col) => col.reuse + col.mentions === 0);
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between">
        <span className={LABEL_CLASS}>Per year</span>
        <span className="flex gap-3 text-xs text-slate-600">
          <span className="flex items-center gap-1.5">
            <span className={`h-2.5 w-2.5 rounded-sm ${LABEL_FILL.REUSE}`} aria-hidden="true" />
            Reuse
          </span>
          <span className="flex items-center gap-1.5">
            <span className={`h-2.5 w-2.5 rounded-sm ${LABEL_FILL.MENTION}`} aria-hidden="true" />
            Mention
          </span>
        </span>
      </div>
      {columns.length === 0 ? (
        <p className="py-6 text-center text-xs text-slate-500">No dated papers yet.</p>
      ) : (
        <div role="img" aria-label={`Reuse and mentions per year. ${splitBarSummary(perYear)}.`} className="flex flex-col gap-1">
          <div className="relative grid h-[88px] items-end gap-1.5 border-b border-slate-300" style={grid}>
            {nothingYet && (
              <span className="absolute inset-0 flex items-center justify-center text-xs text-slate-500">
                No reuse or mentions yet
              </span>
            )}
            {!nothingYet && columns.map((col, i) => (
              <div key={col.year} className="flex h-full items-end justify-center">
                {col.reuse + col.mentions === 0 ? (
                  i === lastIndex && columns.length <= NONE_YET_UP_TO ? (
                    <span className="pb-1 text-[10px] text-slate-500">none yet</span>
                  ) : null
                ) : (
                  <div className="flex w-7 max-w-full flex-col gap-[2px]">
                    {col.mentions > 0 && (
                      <div
                        className={`flex items-center justify-center rounded-t-[3px] ${LABEL_FILL.MENTION} text-[10px] font-semibold tabular-nums text-sky-900`}
                        style={{ height: col.mentionHeight }}
                      >
                        {col.showMentionCount ? col.mentions : null}
                      </div>
                    )}
                    {col.reuse > 0 && (
                      <div
                        className={`flex items-center justify-center ${LABEL_FILL.REUSE} text-[10px] font-semibold tabular-nums text-white ${
                          col.mentions > 0 ? '' : 'rounded-t-[3px]'
                        }`}
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
          <div className="grid gap-1.5 text-center text-[11px] tabular-nums text-slate-600" style={grid}>
            {columns.map((col) => (
              <span key={col.year}>{col.label}</span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

const HOW_WE_COUNT_ID = 'dataset-how-we-count';

/** Grace period (ms) before closing once the pointer leaves, so it can cross to the popover. */
const HOW_WE_COUNT_CLOSE_MS = 150;

/**
 * "How we count": opens on hover, click or tap (Enter from the keyboard), and
 * closes when the pointer leaves it, on a click outside, Escape, or focus
 * moving elsewhere.
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
        aria-expanded={open}
        aria-controls={HOW_WE_COUNT_ID}
        className="text-[13px] text-blue-700 hover:text-blue-900 hover:underline"
      >
        How we count
      </button>
      {open && (
        // Focusable so a click inside it keeps focus within the popover.
        <div
          id={HOW_WE_COUNT_ID}
          role="region"
          aria-label="How we count"
          tabIndex={-1}
          className="absolute right-0 top-full z-20 mt-2 w-[min(320px,calc(100vw-4rem))] rounded-xl border border-slate-200 bg-white p-4 text-left shadow-lg outline-none"
        >
          <dl className="flex flex-col gap-2 text-xs leading-snug text-slate-600">
            <div>
              <dt className="font-semibold text-slate-900">Reuse</dt>
              <dd>The citing paper's full text shows it used this dataset's data.</dd>
            </div>
            <div>
              <dt className="font-semibold text-slate-900">Independent</dt>
              <dd>
                None of its authors are authors of the dataset or its papers, and the classifier didn't judge it the
                dataset's own lab.
              </dd>
            </div>
            <div>
              <dt className="font-semibold text-slate-900">Mention</dt>
              <dd>Cites the dataset or its paper without using the data.</dd>
            </div>
            <div>
              <dt className="font-semibold text-slate-900">Per year</dt>
              <dd>By the citing paper's publication year, from the dataset's release to now.</dd>
            </div>
            <div>
              <dt className="font-semibold text-slate-900">Coverage</dt>
              <dd>
                Papers are labelled from their full text. Citing papers without full text, or not reached yet, aren't
                counted until they are.
              </dd>
            </div>
          </dl>
        </div>
      )}
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
  const published = formatMonthYear(metrics.published);

  return (
    <>
      <div className="flex items-center justify-between">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-500">Dataset impact</h2>
        <HowWeCount />
      </div>

      <div className="grid grid-cols-2 gap-2.5">
        <Stat label="Reuse" value={reuse} valueClassName={`text-[28px] ${reuse > 0 ? 'text-emerald-700' : 'text-slate-600'}`}>
          <div className="text-xs text-slate-600">{reuse === 1 ? 'paper used the data' : 'papers used the data'}</div>
        </Stat>
        <Stat
          label="Independent"
          value={
            reuse > 0 ? (
              <>
                {metrics.independent_reuse_count}{' '}
                <span className="text-[13px] font-medium text-slate-500">of {reuse}</span>
              </>
            ) : (
              '–'
            )
          }
          valueClassName={`text-[28px] ${reuse > 0 ? 'text-slate-900' : 'text-slate-600'}`}
        >
          <div
            className="mt-1.5 flex h-1.5 gap-[2px]"
            title={
              reuse > 0
                ? `${plural(metrics.independent_reuse_count, 'independent reuse', 'independent reuses')}, ${metrics.same_lab_reuse_count} by the dataset's lab`
                : undefined
            }
          >
            {segments.length ? (
              segments.map((s, i) => (
                <div
                  key={`${s.kind}-${i}`}
                  className={`rounded-sm ${s.kind === 'independent' ? 'bg-emerald-700' : 'bg-emerald-700/30'}`}
                  style={{ flexGrow: s.weight }}
                />
              ))
            ) : (
              <div className="flex-grow rounded-sm bg-slate-200" />
            )}
          </div>
          {reuse > 0 && <span className="sr-only">{metrics.same_lab_reuse_count} by the dataset's lab.</span>}
        </Stat>
        <Stat label="Mentions" value={metrics.mention_count.toLocaleString('en-US')}>
          <div className="text-xs text-slate-600">cited without reuse</div>
        </Stat>
        <Stat
          label="Last reused"
          value={last ? formatMonthYear(last.first_date ?? last.publication_date) ?? 'Date unknown' : 'Not yet'}
          valueClassName={last?.first_date ?? last?.publication_date ? 'text-[26px] text-slate-900' : 'text-[22px] text-slate-600'}
        >
          <div className="truncate text-xs text-slate-600">
            {last ? (
              <a
                href={doiUrl(last.doi)}
                target="_blank"
                rel="noopener noreferrer"
                className="text-blue-700 hover:text-blue-900 hover:underline"
                title={last.title ?? undefined}
              >
                {byline ?? 'View paper'}
              </a>
            ) : published ? (
              `published ${published}`
            ) : null}
          </div>
        </Stat>
      </div>

      <div className="flex flex-col gap-1">
        <PerYearChart perYear={metrics.per_year} />
        {undated > 0 && (
          <p className="text-[11px] text-slate-500">Not in the chart: {plural(undated, 'undated paper', 'undated papers')}.</p>
        )}
      </div>

      <p className={`text-xs ${coverage.incomplete ? 'text-amber-700' : 'text-slate-500'}`}>{coverage.text}</p>
    </>
  );
}

/**
 * The dataset page's square "Dataset impact" card: reuse, independent reuse,
 * mentions, last reuse, a per-year split bar chart and how many citing papers
 * the numbers rest on.
 */
export function DatasetImpactCard({
  metrics,
  loading = false,
  error = null,
}: {
  metrics: TrackedDatasetMetrics | null;
  loading?: boolean;
  error?: string | null;
}) {
  if (metrics) {
    return (
      <section aria-label="Dataset impact" className={CARD_CLASS}>
        <ImpactContent metrics={metrics} />
      </section>
    );
  }
  if (error) {
    return (
      <section aria-label="Dataset impact" className="rounded-2xl border border-slate-200 bg-white p-[22px] shadow-xl">
        <h2 className="text-xs font-semibold uppercase tracking-wider text-slate-500">Dataset impact</h2>
        <p className="mt-2 text-sm text-slate-600">Impact metrics couldn't be loaded.</p>
        <p className="mt-1 text-xs text-slate-500">{error}</p>
      </section>
    );
  }
  if (!loading) return null;
  return (
    <section aria-label="Dataset impact" aria-busy="true" className={CARD_CLASS}>
      <div className="h-3 w-28 animate-pulse rounded bg-slate-200" />
      <div className="grid grid-cols-2 gap-2.5">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className="h-[92px] animate-pulse rounded-[10px] bg-slate-100" />
        ))}
      </div>
      <div className="h-[120px] animate-pulse rounded-[10px] bg-slate-100" />
      <div className="h-3 w-3/4 animate-pulse rounded bg-slate-200" />
    </section>
  );
}
