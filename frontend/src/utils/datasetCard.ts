import type { DatasetDetailResponse, TrackedDatasetMetrics } from '../services/api';
import { reuseTypeLabel } from './classification';
import { PAPER_LABELS } from './datasetPapers';
import type { DatasetPaperItem, PaperLabel } from './datasetPapers';
import { plural } from './reuseMetrics';

type DatasetRecord = DatasetDetailResponse['dataset'];

/** Citation rows the dataset detail endpoint returns at most (its LIMIT in api/main.py). */
export const CITATION_LINKS_LOADED = 250;

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

/** "dcite:ContactPerson" -> "Contact Person". */
export function roleLabel(raw: string): string {
  return raw.replace(/^dcite:/, '').replace(/([a-z])([A-Z])/g, '$1 $2');
}

/**
 * Contributors with a role other than author, grouped by role in the order
 * the roles first appear, each name once per role (archives repeat roles,
 * e.g. "Funder, Sponsor, Funder").
 */
export function contributorRoles(
  contributors: DatasetRecord['contributors'],
): Array<{ role: string; names: string[] }> {
  const byRole = new Map<string, string[]>();
  (contributors ?? []).forEach((c) => {
    (c.roles ?? []).forEach((raw) => {
      const role = roleLabel(raw);
      if (/^author$/i.test(role)) return;
      const names = byRole.get(role) ?? [];
      if (!names.includes(c.name)) byRole.set(role, [...names, c.name]);
    });
  });
  return Array.from(byRole.entries()).map(([role, names]) => ({ role, names }));
}

export interface ReuseTypeRow {
  key: string;
  label: string;
  independent: number;
  sameLab: number;
  /** No lab call yet (e.g. before /metrics answers): never counted as independent. */
  unknown: number;
}

/**
 * How this dataset's reuse papers used it, by reuse type, split by lab with
 * the same rule as the paper list (paperLab). Only labelled reuse citations
 * count; one per paper.
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
    const row = rows.get(key) ?? { key, label, independent: 0, sameLab: 0, unknown: 0 };
    const lab = paperLab(p, sameLabByDoi);
    if (lab === true) row.sameLab += 1;
    else if (lab === false) row.independent += 1;
    else row.unknown += 1;
    rows.set(key, row);
  });
  const total = (r: ReuseTypeRow) => r.independent + r.sameLab + r.unknown;
  return Array.from(rows.values()).sort((a, b) => total(b) - total(a) || a.label.localeCompare(b.label));
}

/**
 * Short, true statements derived from the metrics, for
 * the Takeaways part of the notes card. They add to the Impact tiles rather
 * than repeat them, so the lab split and the latest reuse are left out.
 * Empty when there is nothing to say yet.
 */
export function takeaways(ds: DatasetRecord, metrics: TrackedDatasetMetrics | null): string[] {
  const out: string[] = [];
  if (!metrics) return out;
  const reuse = metrics.reuse_count;
  const mentions = metrics.mention_count;
  const classified = metrics.coverage.classified;

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

  // "Most" only when it is more than half of the classified citing papers.
  if (classified > 0 && reuse === 0) {
    out.push(`No reuse found yet among ${plural(classified, 'classified citing paper', 'classified citing papers')}.`);
  } else if (classified > 0 && mentions * 2 > classified) {
    out.push(`Most classified citing papers mention it without reusing it: ${mentions} of ${classified}.`);
  } else if (classified > 0 && reuse * 2 > classified) {
    out.push(`Most classified citing papers reused the data: ${reuse} of ${classified}.`);
  } else if (classified > 0) {
    out.push(`Of ${plural(classified, 'classified citing paper', 'classified citing papers')}, ${reuse} reused the data and ${mentions} mentioned it.`);
  }
  // No "N primary papers with M citing works": summing each paper's count
  // counts a paper that cites two of them twice (816 vs 553 on DANDI:000070).
  return out;
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
    // The detail endpoint returns at most CITATION_LINKS_LOADED rows (api/main.py), one per
    // primary paper and citing paper pair, so a citing paper can take several of them.
    if (citationsShown >= CITATION_LINKS_LOADED) {
      out.push(
        `The paper list and How It Is Reused come from the first ${CITATION_LINKS_LOADED} citation links, one per citing paper and primary paper it cites, so they can hold fewer papers than that. The counts above include every link.`,
      );
    }
  }
  out.push('Reuse and mention labels are assigned by a language model from each citing paper’s full text, with verified quotes.');
  out.push('Citing papers come from OpenAlex, which indexes most but not all of the literature.');
  return out;
}

/** Counts `values` into `bins` equal-width buckets between its min and max, for a column's mini-histogram. */
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

/** "2013 to 2018": the range a column spans; the value alone when there is one. */
export function rangeLabel(values: number[]): string {
  const nums = values.filter((v) => Number.isFinite(v));
  if (!nums.length) return '—';
  const min = Math.min(...nums);
  const max = Math.max(...nums);
  return min === max ? `${min}` : `${min} to ${max}`;
}

/** How the papers split by label, in the filter chips' order, labels with no papers left out. */
export function roleShares(papers: DatasetPaperItem[]): Array<{ label: PaperLabel; share: number; count: number }> {
  const total = papers.length || 1;
  return PAPER_LABELS.map((label) => {
    const count = papers.filter((p) => p.label === label).length;
    return { label, count, share: count / total };
  }).filter((r) => r.count > 0);
}

/**
 * A reuse paper's lab: true for the dataset's own lab, false for a
 * different lab, undefined when unknown (or not a reuse paper). The metrics'
 * call (shared authors or the classifier) wins, so the tag agrees with the
 * Independent count; without it only the classifier's "same lab" is trusted.
 */
export function paperLab(paper: DatasetPaperItem, sameLabByDoi: Map<string, boolean>): boolean | undefined {
  if (paper.label !== 'REUSE') return undefined;
  return sameLabByDoi.get(paper.doi) ?? (paper.citation?.same_lab === true ? true : undefined);
}

/** Reuse papers by lab, with the ones we can't place counted apart. */
export function labSplit(
  papers: DatasetPaperItem[],
  sameLabByDoi: Map<string, boolean>,
): { independent: number; sameLab: number; unknown: number } {
  const out = { independent: 0, sameLab: 0, unknown: 0 };
  papers.forEach((p) => {
    if (p.label !== 'REUSE') return;
    const lab = paperLab(p, sameLabByDoi);
    if (lab === true) out.sameLab += 1;
    else if (lab === false) out.independent += 1;
    else out.unknown += 1;
  });
  return out;
}
