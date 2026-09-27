import type { DatasetDetailCitation, DatasetDetailPaper, WorkVersion } from '../services/api';

/** The labels the dataset page lists papers under, in display order. */
export type PaperLabel = 'PRIMARY' | 'REUSE' | 'MENTION';
export const PAPER_LABELS: PaperLabel[] = ['PRIMARY', 'REUSE', 'MENTION'];

export const PAPER_LABEL_TEXT: Record<PaperLabel, string> = {
  PRIMARY: 'Primary',
  REUSE: 'Reuse',
  MENTION: 'Mention',
};

export const PAPER_LABEL_HELP: Record<PaperLabel, string> = {
  PRIMARY: "The dataset's own publications",
  REUSE: "Used this dataset's data",
  MENTION: 'Cites the dataset or its paper without using the data',
};

/**
 * A citing paper is labelled once per primary paper it cites; it takes the
 * first of these it has (the order the metrics endpoint counts by).
 */
const CITING_PRECEDENCE = ['REUSE', 'PRIMARY', 'MENTION', 'NEITHER'];

export interface DatasetPaperItem {
  /** The DOI the paper is shown as: its published version when it has several. */
  doi: string;
  label: PaperLabel;
  title: string | null;
  authors: string[];
  journal: string | null;
  /** Publication date, or the year alone when that is all there is. */
  date: string | null;
  country: string | null;
  /** Mapped primary papers: how many citing papers were found for it. */
  citingCount?: number;
  openalexId?: string | null;
  /** Classified citing papers: the row whose label the paper takes (evidence, reuse type). */
  citation?: DatasetDetailCitation;
  /** Every version when the paper exists under several DOIs (e.g. a preprint), oldest first. */
  versions: WorkVersion[];
  /** The shown version is a preprint: no published version is known. */
  isPreprint: boolean;
}

function rank(c: DatasetDetailCitation): number {
  return CITING_PRECEDENCE.indexOf((c.classification || '').toUpperCase());
}

function workOf(key: string | null | undefined, doi: string): string {
  return key || `d:${doi.toLowerCase()}`;
}

function groupBy<T>(rows: T[], keyOf: (row: T) => string): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  rows.forEach((row) => {
    const key = keyOf(row);
    groups.set(key, [...(groups.get(key) ?? []), row]);
  });
  return groups;
}

/**
 * One entry per paper: the dataset's mapped primary papers, then the citing
 * papers labelled Reuse, Primary or Mention. The versions of a paper (a
 * preprint and its published version share a work key from the API) are one
 * entry, shown as the published version and labelled by the strongest label
 * any version has. Neither and unclassified citing papers are left out.
 * Sorted by label, then newest first.
 */
export function buildPaperList(
  primaryPapers: DatasetDetailPaper[],
  citations: DatasetDetailCitation[],
): DatasetPaperItem[] {
  const items = new Map<string, DatasetPaperItem>();
  groupBy(primaryPapers, (p) => workOf(p.work_key, p.paper_doi)).forEach((rows, key) => {
    const shown = rows.find((p) => p.paper_doi === p.work_doi) ?? rows[0];
    items.set(key, {
      doi: shown.paper_doi,
      label: 'PRIMARY',
      title: shown.paper_title ?? null,
      authors: shown.authors ?? [],
      journal: shown.journal ?? null,
      date: shown.publication_date ?? (shown.publication_year ? String(shown.publication_year) : null),
      country: shown.senior_author_country ?? null,
      citingCount: rows.reduce((sum, p) => sum + p.citing_papers_count, 0),
      openalexId: shown.openalex_id ?? null,
      versions: shown.work_versions ?? [],
      isPreprint: shown.is_preprint ?? false,
    });
  });
  const primaryDois = new Set(primaryPapers.map((p) => p.paper_doi));

  groupBy(citations, (c) => workOf(c.citing_work_key, c.citing_paper_doi)).forEach((rows, key) => {
    if (items.has(key) || rows.some((c) => primaryDois.has(c.citing_paper_doi))) return;
    const labelled = rows.filter((c) => rank(c) >= 0);
    if (!labelled.length) return;
    const best = labelled.reduce((a, b) => (rank(b) < rank(a) ? b : a));
    const label = (best.classification || '').toUpperCase() as PaperLabel;
    if (!PAPER_LABELS.includes(label)) return;
    const shown = rows.find((c) => c.citing_paper_doi === c.citing_work_doi) ?? best;
    items.set(key, {
      doi: shown.citing_paper_doi,
      label,
      title: shown.citing_paper_title ?? best.citing_paper_title ?? null,
      authors: shown.citing_authors ?? best.citing_authors ?? [],
      journal: shown.citing_journal ?? null,
      date:
        shown.citing_publication_date ?? (shown.citing_publication_year ? String(shown.citing_publication_year) : null),
      country: shown.citing_senior_author_country ?? null,
      citation: best,
      versions: shown.citing_work_versions ?? [],
      isPreprint: shown.citing_is_preprint ?? false,
    });
  });

  return Array.from(items.values()).sort(
    (a, b) =>
      PAPER_LABELS.indexOf(a.label) - PAPER_LABELS.indexOf(b.label) ||
      (b.date ?? '').localeCompare(a.date ?? '') ||
      (a.title ?? a.doi).localeCompare(b.title ?? b.doi),
  );
}

export function countByLabel(items: DatasetPaperItem[]): Record<PaperLabel, number> {
  const counts: Record<PaperLabel, number> = { PRIMARY: 0, REUSE: 0, MENTION: 0 };
  items.forEach((item) => {
    counts[item.label] += 1;
  });
  return counts;
}
