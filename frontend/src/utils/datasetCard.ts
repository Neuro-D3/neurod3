import type { DatasetDetailCitation, DatasetDetailResponse, TrackedDatasetMetrics } from '../services/api';
import { reuseTypeLabel } from './classification';
import type { DatasetPaperItem } from './datasetPapers';
import { plural, surname } from './reuseMetrics';

type DatasetRecord = DatasetDetailResponse['dataset'];

/** "DANDI:000070", "OpenNeuro:ds000001": the archive-qualified identifier. */
export function qualifiedId(source: string, datasetId: string): string {
  return `${source}:${datasetId}`;
}

/** Year of an ISO-ish date, or null. */
export function yearOf(date: string | null | undefined): number | null {
  const m = /^(\d{4})/.exec(date ?? '');
  return m ? Number(m[1]) : null;
}

/**
 * The command that fetches the dataset, for archives with a command-line
 * client; otherwise null and the page shows the archive link instead.
 */
export function downloadCommand(source: string, datasetId: string): string | null {
  switch (source) {
    case 'DANDI':
      return `dandi download DANDI:${datasetId}`;
    case 'OpenNeuro':
      return `openneuro-py download --dataset ${datasetId}`;
    default:
      return null;
  }
}

/** The libraries that read the archive's files; shown as chips. */
export function archiveLibraries(source: string): string[] {
  switch (source) {
    case 'DANDI':
      return ['dandi', 'pynwb'];
    case 'OpenNeuro':
      return ['openneuro-py', 'pybids'];
    case 'SPARC':
      return ['sparc-me'];
    default:
      return [];
  }
}

/** "Churchland, Mark; Cunningham, John P.; …" from the authors list, at most `max` names. */
function citationAuthors(authors: string[], max = 8): string {
  if (!authors.length) return '';
  const names = authors.slice(0, max).join('; ');
  return authors.length > max ? `${names}; et al.` : names;
}

/**
 * A plain-text data citation in the DataCite style:
 * Authors (Year) Title [Data set]. Archive. URL
 */
export function citationText(ds: DatasetRecord): string {
  const authors = citationAuthors(ds.authors ?? []);
  const year = yearOf(ds.created_at);
  const parts = [
    authors ? `${authors}${year ? ` (${year})` : ''}` : year ? `(${year})` : '',
    `${ds.title} [Data set]`,
    `${ds.source} Archive`,
    ds.url ?? '',
  ].filter(Boolean);
  return parts.join('. ').replace(/\.\./g, '.');
}

/** Contributors with a role other than author, as "Role: Name" pairs for the masthead. */
export function contributorRoles(
  contributors: DatasetRecord['contributors'],
  max = 4,
): Array<{ role: string; names: string[] }> {
  const byRole = new Map<string, string[]>();
  (contributors ?? []).forEach((c) => {
    (c.roles ?? []).forEach((raw) => {
      const role = raw.replace(/^dcite:/, '').replace(/([a-z])([A-Z])/g, '$1 $2');
      if (/^author$/i.test(role)) return;
      byRole.set(role, [...(byRole.get(role) ?? []), c.name]);
    });
  });
  return Array.from(byRole.entries())
    .map(([role, names]) => ({ role, names }))
    .slice(0, max);
}

export interface ReuseTypeRow {
  key: string;
  label: string;
  independent: number;
  sameLab: number;
}

/**
 * How this dataset's reuse papers used it, by reuse type, split by lab
 * (the metrics' same-lab call when loaded, else the classifier's). Only
 * labelled reuse citations count; one per paper.
 */
export function reuseTypeBreakdown(
  papers: DatasetPaperItem[],
  sameLabByDoi: Map<string, boolean>,
): ReuseTypeRow[] {
  const rows = new Map<string, ReuseTypeRow>();
  papers.forEach((p) => {
    if (p.label !== 'REUSE' || !p.citation) return;
    const key = (p.citation.reuse_type || '').trim().toUpperCase() || 'UNKNOWN';
    const label = key === 'UNKNOWN' ? 'Not classified' : reuseTypeLabel(p.citation.reuse_type, null) ?? key;
    const row = rows.get(key) ?? { key, label, independent: 0, sameLab: 0 };
    const sameLab = sameLabByDoi.get(p.doi) ?? p.citation.same_lab === true;
    if (sameLab) row.sameLab += 1;
    else row.independent += 1;
    rows.set(key, row);
  });
  return Array.from(rows.values()).sort(
    (a, b) => b.independent + b.sameLab - (a.independent + a.sameLab) || a.label.localeCompare(b.label),
  );
}

/**
 * Short, true statements derived from the metrics and the paper list, for
 * the Takeaways card. Empty when there is nothing to say yet.
 */
export function takeaways(
  ds: DatasetRecord,
  metrics: TrackedDatasetMetrics | null,
  papers: DatasetPaperItem[],
): string[] {
  const out: string[] = [];
  if (!metrics) return out;
  const reuse = metrics.reuse_count;
  const mentions = metrics.mention_count;
  const classified = metrics.coverage.classified;

  if (reuse > 0) {
    out.push(
      `${plural(metrics.independent_reuse_count, 'different-lab reuse paper', 'different-lab reuse papers')} and ${plural(
        metrics.same_lab_reuse_count,
        'from the dataset’s own lab',
        'from the dataset’s own lab',
      )}.`,
    );
  }

  const firstReuseYear = metrics.per_year.find((y) => y.reuse > 0)?.year ?? null;
  const releaseYear = yearOf(metrics.published ?? ds.created_at);
  if (firstReuseYear !== null && releaseYear !== null) {
    const gap = firstReuseYear - releaseYear;
    out.push(
      gap <= 0
        ? `First reuse came in ${firstReuseYear}, the year of release.`
        : `First reuse came in ${firstReuseYear}, ${plural(gap, 'year', 'years')} after release.`,
    );
  }

  if (classified > 0 && mentions > 0 && mentions >= reuse) {
    out.push(`Most citing papers mention it without reusing it: ${mentions} of ${classified} classified.`);
  } else if (classified > 0 && reuse > mentions) {
    out.push(`Most classified citing papers reused the data: ${reuse} of ${classified}.`);
  }

  const last = metrics.last_reuse;
  if (last) {
    const who = surname(last.first_author);
    const year = yearOf(last.first_date ?? last.publication_date);
    if (who && year) out.push(`Most recently reused by ${who}${last.author_count > 1 ? ' et al.' : ''} in ${year}.`);
  }

  if (reuse === 0 && classified > 0) {
    out.push(`No reuse found yet among ${plural(classified, 'classified citing paper', 'classified citing papers')}.`);
  }

  const primary = papers.filter((p) => p.label === 'PRIMARY');
  const citing = primary.reduce((n, p) => n + (p.citingCount ?? 0), 0);
  if (primary.length > 0 && citing > 0) {
    out.push(`${plural(primary.length, 'primary paper', 'primary papers')} with ${plural(citing, 'citing work', 'citing works')} between them.`);
  }
  return out.slice(0, 4);
}

/**
 * What to know before trusting the numbers: coverage gaps, how labels are
 * made, and the dataset's own status. Always at least the two fixed notes.
 */
export function caveats(
  ds: DatasetRecord,
  metrics: TrackedDatasetMetrics | null,
  metricsTracked: boolean,
  citationsShown: number,
): string[] {
  const out: string[] = [];
  if (ds.dataset_status === 'excluded') {
    out.push('Flagged as a test, placeholder or empty upload; hidden from the dataset list and archive counts.');
  }
  if (!metricsTracked) {
    out.push(`Reuse metrics are not tracked for ${ds.source} yet: no citing papers have been mapped for this archive.`);
  }
  if (metrics) {
    const c = metrics.coverage;
    if (c.no_full_text > 0 || c.pending > 0) {
      const gaps = [
        c.no_full_text > 0 ? `${c.no_full_text.toLocaleString('en-US')} had no full text to classify` : '',
        c.pending > 0 ? `${c.pending.toLocaleString('en-US')} still to classify` : '',
      ].filter(Boolean);
      out.push(`Impact counts are lower bounds: of ${plural(c.citing_papers, 'citing paper', 'citing papers')}, ${gaps.join(' and ')}.`);
    }
    const undated = metrics.undated.reuse + metrics.undated.mentions;
    if (undated > 0) out.push(`${plural(undated, 'labelled paper has', 'labelled papers have')} no publication date and are not in the per-year chart.`);
    if (citationsShown >= 250) out.push('The paper list shows the first 250 citing papers; the counts above include all of them.');
  }
  out.push('Reuse and mention labels are assigned by a language model from each citing paper’s full text, with verified quotes.');
  out.push('Citing papers come from OpenAlex, which indexes most but not all of the literature.');
  if (ds.num_subjects != null && ds.num_subjects > 0 && ds.num_subjects <= 3) {
    out.push(`${plural(ds.num_subjects, 'subject limits', 'subjects limit')} cross-subject generalization.`);
  }
  return out;
}

/** Counts `values` into `bins` equal-width buckets between its min and max, for a header mini-histogram. */
export function histogram(values: number[], bins = 6): number[] {
  const nums = values.filter((v) => Number.isFinite(v));
  if (!nums.length) return [];
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const counts = new Array(bins).fill(0);
  nums.forEach((v) => {
    const i = max === min ? 0 : Math.min(bins - 1, Math.floor(((v - min) / (max - min)) * bins));
    counts[i] += 1;
  });
  return counts;
}

/** "66 to 97 characters", "2013 to 2018", "48 to 137": the range a column spans. */
export function rangeLabel(values: number[], unit = ''): string {
  const nums = values.filter((v) => Number.isFinite(v));
  if (!nums.length) return '—';
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  const u = unit ? ` ${unit}` : '';
  return min === max ? `${min}${u}` : `${min} to ${max}${u}`;
}

/** Per-paper role for the paper map: label counts in label order. */
export function roleShares(papers: DatasetPaperItem[]): Array<{ label: DatasetPaperItem['label']; share: number; count: number }> {
  const total = papers.length || 1;
  return (['REUSE', 'PRIMARY', 'MENTION'] as const)
    .map((label) => {
      const count = papers.filter((p) => p.label === label).length;
      return { label, count, share: count / total };
    })
    .filter((r) => r.count > 0);
}

/** The citations of the Reuse label only, for the reuse-type breakdown. */
export function reuseCitations(citations: DatasetDetailCitation[]): DatasetDetailCitation[] {
  return citations.filter((c) => (c.classification || '').toUpperCase() === 'REUSE');
}
