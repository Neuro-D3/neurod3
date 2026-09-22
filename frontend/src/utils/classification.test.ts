import {
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
  it('treats REUSE and the legacy SECONDARY as reuse, case-insensitively', () => {
    expect(isReuseClassification('REUSE')).toBe(true);
    expect(isReuseClassification('reuse')).toBe(true);
    expect(isReuseClassification('SECONDARY')).toBe(true);
  });
  it('rejects every other label and empties', () => {
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
  it('keeps the legacy SECONDARY green during the reclassification window', () => {
    expect(statusBadgeClass('SECONDARY')).toBe(statusBadgeClass('REUSE'));
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
    expect(statusLabel('SECONDARY')).toBe('Reuse (legacy)');
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
