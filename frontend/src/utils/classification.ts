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
  if (tier === 'high') return { tier, text: 'High confidence', color: 'text-reuse-ink', bg: 'bg-reuse-tint border-reuse-soft' };
  if (tier === 'medium') return { tier, text: 'Medium confidence', color: 'text-warn-ink', bg: 'bg-warn-tint border-warn' };
  if (tier === 'low') return { tier, text: 'Low confidence', color: 'text-danger-ink', bg: 'bg-danger-tint border-danger' };
  return { tier, text: '', color: 'text-muted', bg: 'bg-panel border-line' };
}

export function confidenceShort(value?: number | null): { text: string; color: string } {
  const tier = confidenceTier(value);
  if (tier === 'high') return { text: `High (${value}/10)`, color: 'text-reuse-ink' };
  if (tier === 'medium') return { text: `Medium (${value}/10)`, color: 'text-warn-ink' };
  if (tier === 'low') return { text: `Low (${value}/10)`, color: 'text-danger-ink' };
  return { text: '—', color: 'text-muted' };
}

/**
 * Tailwind classes for a classification / row-status badge: a solid tint with
 * dark text of the same hue (tailwind.config.js). Reuse is blue, mention is
 * orange and primary is navy, the chart hues; everything else is neutral or
 * a warning tone.
 */
export function statusBadgeClass(status?: string | null): string {
  const s = (status || '').trim().toLowerCase();
  if (s === 'reuse') return 'bg-reuse-tint text-reuse-ink ring-reuse-soft';
  if (s === 'primary') return 'bg-primary-tint text-primary-ink ring-primary-soft';
  if (s === 'mention') return 'bg-mention-tint text-mention-ink ring-mention-soft';
  if (s === 'neither') return 'bg-neither-tint text-neither-ink ring-line';
  if (s === 'error') return 'bg-danger-tint text-danger-ink ring-danger';
  if (s === 'no_full_text') return 'bg-tan-tint text-tan-ink ring-line-rule';
  if (s === 'placeholder' || s === 'unknown' || s === 'unclassified') return 'bg-warn-tint text-warn-ink ring-warn';
  if (s.includes('reuse')) return 'bg-reuse-tint text-reuse-ink ring-reuse-soft';
  if (s.includes('mention')) return 'bg-mention-tint text-mention-ink ring-mention-soft';
  return 'bg-neither-tint text-neither-ink ring-line';
}

/**
 * Solid fills in the same hues as statusBadgeClass, for charts and dots: reuse
 * and primary are dark enough to carry white text, mentions carry dark text.
 */
export const LABEL_FILL: Record<'REUSE' | 'MENTION' | 'PRIMARY', string> = {
  REUSE: 'bg-reuse',
  MENTION: 'bg-mention',
  PRIMARY: 'bg-primary',
};

/**
 * Solid fill for any classification or status bucket, for chart segments and
 * legend dots: the label hues above, neutral grey for Neither, the warning
 * tones for the rest.
 */
export function statusFillClass(status?: string | null): string {
  const s = (status || '').trim().toLowerCase();
  if (s === 'reuse') return LABEL_FILL.REUSE;
  if (s === 'primary') return LABEL_FILL.PRIMARY;
  if (s === 'mention') return LABEL_FILL.MENTION;
  if (s === 'error') return 'bg-danger';
  if (s === 'no_full_text') return 'bg-tan-ink';
  if (s === 'placeholder' || s === 'unknown' || s === 'unclassified') return 'bg-warn';
  return 'bg-bar';
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
 * Outcome buckets for the dashboard's distribution bar, in drawing order. The
 * bar uses the same light tints as the label badges (statusBadgeClass), so a
 * label looks the same everywhere on the page.
 */
export const OUTCOME_ORDER: string[] = ['REUSE', 'PRIMARY', 'MENTION', 'NEITHER', 'no_full_text', 'error'];

/** Rows that hold no attempt: created by mapping, or a dry run. */
const NOT_ATTEMPTED = new Set(['placeholder', 'unclassified', 'dry_run']);

export interface OutcomeSlice {
  key: string;
  label: string;
  /** Tailwind classes, from statusBadgeClass. */
  className: string;
  /** Solid fill for the bar, from statusFillClass. */
  fill: string;
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
 * buckets outside OUTCOME_ORDER (should any appear) are kept, drawn last.
 */
export function classificationProgress(
  citationEdges: number,
  byClassification: Record<string, number>,
): ClassificationProgress {
  const counts = Object.entries(byClassification || {}).filter(([k, n]) => !NOT_ATTEMPTED.has(k) && n > 0);
  const attempted = counts.reduce((sum, [, n]) => sum + n, 0);
  const total = Math.max(citationEdges || 0, attempted);
  const outcomes = counts
    .map(([key, count]) => {
      const i = OUTCOME_ORDER.indexOf(key);
      return {
        key,
        label: statusLabel(key),
        className: statusBadgeClass(key),
        fill: statusFillClass(key),
        count,
        share: attempted ? count / attempted : 0,
        order: i >= 0 ? i : OUTCOME_ORDER.length,
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

/**
 * The quote to show as evidence: the first one verified in the paper text.
 * A quote the verifier could not find (`match_type: 'not_found'`) is never
 * shown as evidence; the page reports those as a count instead. A quote with
 * no match_type (never verified) is shown only when nothing verified exists.
 */
export function primaryQuote(quotes?: EvidenceQuote[] | null): EvidenceQuote | null {
  if (!Array.isArray(quotes) || quotes.length === 0) return null;
  const usable = quotes.filter(
    (q) => q && typeof q.quote === 'string' && q.quote.trim().length > 0 && q.match_type !== 'not_found',
  );
  return usable.find((q) => q.match_type) ?? usable[0] ?? null;
}
