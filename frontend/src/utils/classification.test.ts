import {
  classificationProgress,
  confidenceLabel,
  confidenceShort,
  confidenceTier,
  isReuseClassification,
  modalityLabel,
  primaryQuote,
  reuseTypeLabel,
  reusedModalities,
  statusBadgeClass,
  statusLabel,
} from './classification';

describe('isReuseClassification', () => {
  it('treats REUSE as reuse, case-insensitively', () => {
    expect(isReuseClassification('REUSE')).toBe(true);
    expect(isReuseClassification('reuse')).toBe(true);
  });
  it('rejects every other label and empties, including the retired SECONDARY', () => {
    expect(isReuseClassification('SECONDARY')).toBe(false);
    expect(isReuseClassification('MENTION')).toBe(false);
    expect(isReuseClassification('PRIMARY')).toBe(false);
    expect(isReuseClassification('NEITHER')).toBe(false);
    expect(isReuseClassification('')).toBe(false);
    expect(isReuseClassification(null)).toBe(false);
    expect(isReuseClassification(undefined)).toBe(false);
  });
});

describe('confidence on the 1–10 scale', () => {
  it('buckets high / medium / low', () => {
    expect(confidenceTier(10)).toBe('high');
    expect(confidenceTier(7)).toBe('high');
    expect(confidenceTier(6)).toBe('medium');
    expect(confidenceTier(4)).toBe('medium');
    expect(confidenceTier(3)).toBe('low');
    expect(confidenceTier(1)).toBe('low');
  });
  it('has no tier for missing or zero (ERROR rows carry 0)', () => {
    expect(confidenceTier(0)).toBe('none');
    expect(confidenceTier(null)).toBe('none');
    expect(confidenceTier(undefined)).toBe('none');
    expect(confidenceLabel(null).text).toBe('');
    expect(confidenceShort(null).text).toBe('—');
  });
  it('labels carry the tier and the raw score', () => {
    expect(confidenceLabel(9).text).toBe('High confidence');
    expect(confidenceLabel(5).tier).toBe('medium');
    expect(confidenceShort(9).text).toBe('High (9/10)');
    expect(confidenceShort(2).text).toBe('Low (2/10)');
  });
});

describe('statusBadgeClass', () => {
  it('colours the new labels and statuses distinctly', () => {
    expect(statusBadgeClass('REUSE')).toContain('emerald');
    expect(statusBadgeClass('MENTION')).toContain('sky');
    expect(statusBadgeClass('PRIMARY')).toContain('blue');
    expect(statusBadgeClass('NEITHER')).toContain('slate');
    expect(statusBadgeClass('error')).toContain('rose');
    expect(statusBadgeClass('no_full_text')).toContain('orange');
    expect(statusBadgeClass('placeholder')).toContain('amber');
  });
  it('no longer special-cases the retired SECONDARY label', () => {
    expect(statusBadgeClass('SECONDARY')).toContain('slate');
  });
  it('falls back to neutral for unknown buckets', () => {
    expect(statusBadgeClass('something_new')).toContain('slate');
    expect(statusBadgeClass(null)).toContain('slate');
  });
});

describe('statusLabel', () => {
  it('humanizes known buckets', () => {
    expect(statusLabel('REUSE')).toBe('Reuse');
    expect(statusLabel('no_full_text')).toBe('No full text');
    expect(statusLabel('SECONDARY')).toBe('SECONDARY');
    expect(statusLabel('')).toBe('Unclassified');
    expect(statusLabel('brand_new')).toBe('brand new');
  });
});

describe('reuseTypeLabel', () => {
  it('humanizes the vocabulary and surfaces the OTHER write-in', () => {
    expect(reuseTypeLabel('NOVEL_ANALYSIS')).toBe('Novel analysis');
    expect(reuseTypeLabel('ml_training')).toBe('ML training');
    expect(reuseTypeLabel('OTHER', 'stimulus design reference')).toBe('Other: stimulus design reference');
    expect(reuseTypeLabel('OTHER')).toBe('Other');
    expect(reuseTypeLabel('SOMETHING_ELSE')).toBe('Something else');
    expect(reuseTypeLabel(null)).toBeNull();
  });
});

describe('modalities', () => {
  it('labels the vocabulary', () => {
    expect(modalityLabel('neurophysiology')).toBe('Neurophysiology');
    expect(modalityLabel('other')).toBe('Other data');
    expect(modalityLabel('weird')).toBe('weird');
  });
  it('normalizes the JSONB payload shapes', () => {
    expect(reusedModalities(['neurophysiology', 'behavior'])).toEqual(['neurophysiology', 'behavior']);
    expect(reusedModalities('["imaging"]')).toEqual(['imaging']);
    expect(reusedModalities('imaging')).toEqual(['imaging']);
    expect(reusedModalities(null)).toEqual([]);
    expect(reusedModalities([1, '', 'x'])).toEqual(['x']);
  });
});

describe('primaryQuote', () => {
  it('prefers a quote that was found in the paper', () => {
    const q = primaryQuote([
      { quote: 'fabricated', match_type: 'not_found' },
      { quote: 'we downloaded the data', match_type: 'exact' },
    ]);
    expect(q?.quote).toBe('we downloaded the data');
  });
  it('falls back to the first non-empty quote and handles empties', () => {
    expect(primaryQuote([{ quote: '' }, { quote: 'x', match_type: 'not_found' }])?.quote).toBe('x');
    expect(primaryQuote([])).toBeNull();
    expect(primaryQuote(null)).toBeNull();
  });
});

describe('classificationProgress', () => {
  const live = { MENTION: 162, no_full_text: 116, error: 9, NEITHER: 7, REUSE: 3 };

  it('splits all edges into attempted and not yet', () => {
    const p = classificationProgress(15038, live);
    expect(p.attempted).toBe(297);
    expect(p.notYet).toBe(15038 - 297);
    expect(p.attemptedShare).toBeCloseTo(297 / 15038);
  });

  it('orders outcomes by the fixed palette order, not by count', () => {
    const keys = classificationProgress(15038, live).outcomes.map((o) => o.key);
    expect(keys).toEqual(['REUSE', 'MENTION', 'NEITHER', 'no_full_text', 'error']);
  });

  it('gives outcome shares of attempted edges', () => {
    const reuse = classificationProgress(15038, live).outcomes[0];
    expect(reuse.share).toBeCloseTo(3 / 297);
    expect(reuse.label).toBe('Reuse');
    // Same tint as the label badge elsewhere on the page.
    expect(reuse.className).toBe(statusBadgeClass('REUSE'));
  });

  it('does not count placeholder or dry-run rows as attempted', () => {
    const p = classificationProgress(100, { placeholder: 40, dry_run: 5, MENTION: 10 });
    expect(p.attempted).toBe(10);
    expect(p.outcomes.map((o) => o.key)).toEqual(['MENTION']);
  });

  it('keeps an unknown bucket instead of dropping it', () => {
    const p = classificationProgress(10, { REUSE: 1, SOMETHING_NEW: 2 });
    expect(p.outcomes.map((o) => o.key)).toEqual(['REUSE', 'SOMETHING_NEW']);
  });

  it('handles no data', () => {
    const p = classificationProgress(0, {});
    expect(p).toEqual({ total: 0, attempted: 0, notYet: 0, attemptedShare: 0, outcomes: [] });
  });
});
