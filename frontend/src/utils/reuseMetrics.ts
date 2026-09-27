import type { DatasetMetricsCoverage, DatasetMetricsYear } from '../services/api';

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

/** "3 reuses", "1 mention", with thousands separators. */
export function plural(n: number, one: string, many: string): string {
  return `${n.toLocaleString('en-US')} ${n === 1 ? one : many}`;
}

/**
 * "2023-11-24" and "2023-11" become "Nov 2023"; "2023" stays "2023". Parsed by
 * hand because `new Date("2023-11")` is UTC midnight, which is the previous
 * month west of Greenwich.
 */
export function formatMonthYear(date: string | null | undefined): string | null {
  if (!date) return null;
  const match = /^(\d{4})(?:-(\d{2}))?/.exec(date);
  if (!match) return date;
  const month = match[2] ? MONTHS[Number(match[2]) - 1] : undefined;
  return month ? `${month} ${match[1]}` : match[1];
}

/**
 * A dataset's publication date as a page shows it: the full date, or the year
 * alone when only the year is known (precision "year", e.g. most of CRCNS).
 */
export function formatPublishedDate(date: string | null | undefined, precision?: string | null): string | null {
  if (!date) return null;
  if (precision === 'year') return date.slice(0, 4);
  const parsed = new Date(date);
  return Number.isNaN(parsed.getTime()) ? date : parsed.toLocaleDateString();
}

/** Surname from "First M. Last" or "Last, First"; invisible formatting characters are dropped. */
export function surname(name: string | null | undefined): string | null {
  const clean = (name ?? '').replace(/\p{Cf}/gu, '').trim();
  if (!clean) return null;
  if (clean.includes(',')) return clean.split(',')[0].trim() || null;
  const words = clean.split(/\s+/);
  return words[words.length - 1];
}

/** "Farashi et al." for a paper with several authors, "Farashi" for one. */
export function paperByline(firstAuthor: string | null | undefined, authorCount: number): string | null {
  const last = surname(firstAuthor);
  if (!last) return null;
  return authorCount > 1 ? `${last} et al.` : last;
}

export interface SplitBarColumn {
  year: number;
  /** "2023", or "'23" when there are too many years for full labels. */
  label: string;
  reuse: number;
  mentions: number;
  reuseHeight: number;
  mentionHeight: number;
  showReuseCount: boolean;
  showMentionCount: boolean;
}

/** Segments shorter than this (px) don't show their count inside. */
export const SEGMENT_LABEL_MIN_PX = 14;
/** A non-zero segment is never drawn thinner than this (px), so a single reuse stays visible. */
export const SEGMENT_MIN_PX = 3;

/**
 * Columns for the per-year split bar chart. Each year's bar is its reuse
 * (bottom) and mentions (top) on one scale, where the busiest year is
 * `maxHeight` px tall.
 */
export function splitBarColumns(perYear: DatasetMetricsYear[], maxHeight: number, fullLabelsUpTo = 10): SplitBarColumn[] {
  const unit = maxHeight / Math.max(1, ...perYear.map((y) => y.reuse + y.mentions));
  const segment = (count: number) => (count > 0 ? Math.max(SEGMENT_MIN_PX, count * unit) : 0);
  const shortLabels = perYear.length > fullLabelsUpTo;
  return perYear.map((y) => {
    const reuseHeight = segment(y.reuse);
    const mentionHeight = segment(y.mentions);
    return {
      year: y.year,
      label: shortLabels ? `'${String(y.year).slice(2)}` : String(y.year),
      reuse: y.reuse,
      mentions: y.mentions,
      reuseHeight,
      mentionHeight,
      showReuseCount: reuseHeight >= SEGMENT_LABEL_MIN_PX,
      showMentionCount: mentionHeight >= SEGMENT_LABEL_MIN_PX,
    };
  });
}

/** Text alternative for the chart: only the years with papers. */
export function splitBarSummary(perYear: DatasetMetricsYear[]): string {
  const years = perYear.filter((y) => y.reuse + y.mentions > 0);
  if (!years.length) return 'No reuse or mentions yet';
  return years
    .map((y) => `${y.year}: ${plural(y.reuse, 'reuse', 'reuses')}, ${plural(y.mentions, 'mention', 'mentions')}`)
    .join('; ');
}

export interface IndependenceSegment {
  kind: 'independent' | 'same_lab';
  weight: number;
}

/**
 * Segments for the independent / same-lab bar: one per paper while there are
 * few enough to count by eye, otherwise one proportional segment each.
 */
export function independenceSegments(independent: number, sameLab: number, perPaperUpTo = 12): IndependenceSegment[] {
  if (independent + sameLab <= perPaperUpTo) {
    return [
      ...Array.from({ length: independent }, () => ({ kind: 'independent' as const, weight: 1 })),
      ...Array.from({ length: sameLab }, () => ({ kind: 'same_lab' as const, weight: 1 })),
    ];
  }
  return [
    { kind: 'independent' as const, weight: independent },
    { kind: 'same_lab' as const, weight: sameLab },
  ].filter((s) => s.weight > 0);
}

/** One line on how much of the literature the numbers rest on; `incomplete` while papers are still to classify. */
export function coverageSummary(coverage: DatasetMetricsCoverage): { text: string; incomplete: boolean } {
  if (coverage.citing_papers === 0) return { text: 'No citing papers found yet.', incomplete: false };
  const parts = [`${coverage.classified.toLocaleString('en-US')} classified`];
  if (coverage.no_full_text) parts.push(`${coverage.no_full_text.toLocaleString('en-US')} without full text`);
  if (coverage.pending) parts.push(`${coverage.pending.toLocaleString('en-US')} still to classify`);
  return {
    text: `${plural(coverage.citing_papers, 'citing paper', 'citing papers')} found: ${parts.join(', ')}.`,
    incomplete: coverage.pending > 0,
  };
}
