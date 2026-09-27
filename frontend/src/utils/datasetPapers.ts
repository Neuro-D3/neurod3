import type { DatasetDetailCitation, DatasetDetailPaper } from '../services/api';

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
}

function rank(c: DatasetDetailCitation): number {
  return CITING_PRECEDENCE.indexOf((c.classification || '').toUpperCase());
}

/**
 * One entry per paper: the dataset's mapped primary papers, then the citing
 * papers labelled Reuse, Primary or Mention. Neither and unclassified citing
 * papers are left out. Sorted by label, then newest first.
 */
export function buildPaperList(
  primaryPapers: DatasetDetailPaper[],
  citations: DatasetDetailCitation[],
): DatasetPaperItem[] {
  const items = new Map<string, DatasetPaperItem>();
  primaryPapers.forEach((p) => {
    items.set(p.paper_doi, {
      doi: p.paper_doi,
      label: 'PRIMARY',
      title: p.paper_title ?? null,
      authors: p.authors ?? [],
      journal: p.journal ?? null,
      date: p.publication_date ?? (p.publication_year ? String(p.publication_year) : null),
      country: p.senior_author_country ?? null,
      citingCount: p.citing_papers_count,
      openalexId: p.openalex_id ?? null,
    });
  });

  const best = new Map<string, DatasetDetailCitation>();
  citations.forEach((c) => {
    if (rank(c) < 0) return;
    const current = best.get(c.citing_paper_doi);
    if (!current || rank(c) < rank(current)) best.set(c.citing_paper_doi, c);
  });
  best.forEach((c, doi) => {
    const label = (c.classification || '').toUpperCase() as PaperLabel;
    if (items.has(doi) || !PAPER_LABELS.includes(label)) return;
    items.set(doi, {
      doi,
      label,
      title: c.citing_paper_title ?? null,
      authors: c.citing_authors ?? [],
      journal: c.citing_journal ?? null,
      date: c.citing_publication_date ?? (c.citing_publication_year ? String(c.citing_publication_year) : null),
      country: c.citing_senior_author_country ?? null,
      citation: c,
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
