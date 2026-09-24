/**
 * Display helpers for paper reuse classifications.
 *
 * The classifier (airflow/dags/utils/classify_fulltext_reuse.py) labels each
 * (citing paper, dataset) pair REUSE / MENTION / NEITHER in citing mode or
 * PRIMARY / REUSE / NEITHER in direct mode, with a 1–10 confidence and
 * verified evidence quotes. (The previous excerpt-based classifier's SECONDARY
 * label was retired once every row had been reclassified.)
 */

export const REUSE_LABELS = ['REUSE'] as const;

export function isReuseClassification(classification?: string | null): boolean {
  const c = (classification || '').trim().toUpperCase();
  return (REUSE_LABELS as readonly string[]).includes(c);
}

export type ConfidenceTier = 'high' | 'medium' | 'low' | 'none';

/** Confidence is 1–10 from the whole-paper classifier; the old classifier used 1–3. */
export function confidenceTier(value?: number | null): ConfidenceTier {
  if (value == null || Number.isNaN(value)) return 'none';
  if (value >= 7) return 'high';
  if (value >= 4) return 'medium';
  if (value >= 1) return 'low';
  return 'none';
}

export const CONFIDENCE_TIERS: ConfidenceTier[] = ['high', 'medium', 'low'];

export function confidenceLabel(value?: number | null): { tier: ConfidenceTier; text: string; color: string; bg: string } {
  const tier = confidenceTier(value);
  if (tier === 'high') return { tier, text: 'High confidence', color: 'text-emerald-700', bg: 'bg-emerald-50 border-emerald-200' };
  if (tier === 'medium') return { tier, text: 'Medium confidence', color: 'text-amber-700', bg: 'bg-amber-50 border-amber-200' };
  if (tier === 'low') return { tier, text: 'Low confidence', color: 'text-red-600', bg: 'bg-red-50 border-red-200' };
  return { tier, text: '', color: 'text-slate-400', bg: 'bg-slate-50 border-slate-200' };
}

export function confidenceShort(value?: number | null): { text: string; color: string } {
  const tier = confidenceTier(value);
  if (tier === 'high') return { text: `High (${value}/10)`, color: 'text-emerald-600' };
  if (tier === 'medium') return { text: `Medium (${value}/10)`, color: 'text-amber-600' };
  if (tier === 'low') return { text: `Low (${value}/10)`, color: 'text-red-500' };
  return { text: '—', color: 'text-slate-400' };
}

/** Tailwind classes for a classification / row-status badge. */
export function statusBadgeClass(status?: string | null): string {
  const s = (status || '').trim().toLowerCase();
  if (s === 'reuse') return 'bg-emerald-500/15 text-emerald-700 ring-emerald-500/30';
  if (s === 'primary') return 'bg-blue-500/15 text-blue-700 ring-blue-500/30';
  if (s === 'mention') return 'bg-sky-500/15 text-sky-700 ring-sky-500/30';
  if (s === 'neither') return 'bg-slate-500/10 text-slate-600 ring-slate-400/30';
  if (s === 'error') return 'bg-rose-500/15 text-rose-700 ring-rose-500/30';
  if (s === 'no_full_text') return 'bg-orange-500/15 text-orange-700 ring-orange-500/30';
  if (s === 'placeholder' || s === 'unknown' || s === 'unclassified') return 'bg-amber-500/15 text-amber-700 ring-amber-500/30';
  if (s.includes('reuse')) return 'bg-emerald-500/15 text-emerald-700 ring-emerald-500/30';
  if (s.includes('mention')) return 'bg-sky-500/15 text-sky-700 ring-sky-500/30';
  return 'bg-slate-500/10 text-slate-700 ring-slate-400/30';
}

/** Human-readable text for a classification or status bucket. */
export function statusLabel(status?: string | null): string {
  const s = (status || '').trim();
  if (!s) return 'Unclassified';
  const known: Record<string, string> = {
    REUSE: 'Reuse',
    MENTION: 'Mention',
    NEITHER: 'Neither',
    PRIMARY: 'Primary',
    error: 'Error',
    no_full_text: 'No full text',
    placeholder: 'Placeholder',
    unclassified: 'Unclassified',
    dry_run: 'Dry run',
  };
  return known[s] ?? s.replace(/_/g, ' ');
}

/**
 * Outcome buckets for the dashboard's distribution bar, in drawing order.
 * Colors are validated as a categorical set (dataviz validate_palette.js, light
 * surface): the order matters, since adjacent segments must stay distinguishable.
 */
export const OUTCOME_BUCKETS: { key: string; color: string }[] = [
  { key: 'REUSE', color: '#059669' },
  { key: 'PRIMARY', color: '#2563eb' },
  { key: 'MENTION', color: '#0891b2' },
  { key: 'no_full_text', color: '#ea580c' },
  { key: 'NEITHER', color: '#7c3aed' },
  { key: 'error', color: '#e11d48' },
];

/** Rows that hold no attempt: created by mapping, or a dry run. */
const NOT_ATTEMPTED = new Set(['placeholder', 'unclassified', 'dry_run']);

export interface OutcomeSlice {
  key: string;
  label: string;
  color: string;
  count: number;
  /** Share of attempted edges, 0–1. */
  share: number;
}

export interface ClassificationProgress {
  total: number;
  attempted: number;
  notYet: number;
  /** Share of all edges attempted, 0–1. */
  attemptedShare: number;
  outcomes: OutcomeSlice[];
}

/**
 * Split all citation edges into attempted vs not yet, and the attempted ones
 * into outcome buckets. `byClassification` is the summary API's bucket counts;
 * buckets without a fixed color (should any appear) are kept, in slate.
 */
export function classificationProgress(
  citationEdges: number,
  byClassification: Record<string, number>,
): ClassificationProgress {
  const counts = Object.entries(byClassification || {}).filter(([k, n]) => !NOT_ATTEMPTED.has(k) && n > 0);
  const attempted = counts.reduce((sum, [, n]) => sum + n, 0);
  const total = Math.max(citationEdges || 0, attempted);
  const known = new Map(OUTCOME_BUCKETS.map((b, i) => [b.key, { ...b, i }]));
  const outcomes = counts
    .map(([key, count]) => {
      const b = known.get(key);
      return {
        key,
        label: statusLabel(key),
        color: b?.color ?? '#64748b',
        count,
        share: attempted ? count / attempted : 0,
        order: b ? b.i : OUTCOME_BUCKETS.length,
      };
    })
    .sort((a, b) => a.order - b.order || b.count - a.count)
    .map(({ order, ...slice }) => slice);
  return {
    total,
    attempted,
    notYet: total - attempted,
    attemptedShare: total ? attempted / total : 0,
    outcomes,
  };
}

const REUSE_TYPE_LABELS: Record<string, string> = {
  TOOL_DEMO: 'Tool demo',
  BENCHMARK: 'Benchmark',
  AGGREGATION: 'Aggregation',
  CONFIRMATORY: 'Confirmatory',
  NOVEL_ANALYSIS: 'Novel analysis',
  ML_TRAINING: 'ML training',
  SIMULATION: 'Simulation',
  TEACHING: 'Teaching',
  OTHER: 'Other',
};

/** "NOVEL_ANALYSIS" -> "Novel analysis"; OTHER shows the write-in when present. */
export function reuseTypeLabel(reuseType?: string | null, other?: string | null): string | null {
  const t = (reuseType || '').trim().toUpperCase();
  if (!t) return null;
  if (t === 'OTHER' && other && other.trim()) return `Other: ${other.trim()}`;
  if (REUSE_TYPE_LABELS[t]) return REUSE_TYPE_LABELS[t];
  return t.toLowerCase().replace(/_/g, ' ').replace(/^\w/, (c) => c.toUpperCase());
}

const MODALITY_LABELS: Record<string, string> = {
  neurophysiology: 'Neurophysiology',
  behavior: 'Behavior',
  imaging: 'Imaging',
  morphology: 'Morphology',
  transcriptomics: 'Transcriptomics',
  other: 'Other data',
  unclear: 'Unclear',
};

export function modalityLabel(modality: string): string {
  const m = modality.trim().toLowerCase();
  return MODALITY_LABELS[m] ?? modality;
}

/** Normalize the JSONB list (or a stray string / null) to a clean array. */
export function reusedModalities(raw: unknown): string[] {
  if (Array.isArray(raw)) return raw.filter((m): m is string => typeof m === 'string' && m.trim().length > 0);
  if (typeof raw === 'string' && raw.trim()) {
    try {
      const parsed = JSON.parse(raw);
      return Array.isArray(parsed) ? reusedModalities(parsed) : [raw];
    } catch {
      return [raw];
    }
  }
  return [];
}

export interface EvidenceQuote {
  quote: string;
  match_type?: string;
  verbatim?: boolean;
  chars?: number;
  offset?: number;
}

/** First usable quote, preferring ones actually found in the paper. */
export function primaryQuote(quotes?: EvidenceQuote[] | null): EvidenceQuote | null {
  if (!Array.isArray(quotes) || quotes.length === 0) return null;
  const usable = quotes.filter((q) => q && typeof q.quote === 'string' && q.quote.trim().length > 0);
  if (usable.length === 0) return null;
  return usable.find((q) => q.match_type && q.match_type !== 'not_found') ?? usable[0];
}
