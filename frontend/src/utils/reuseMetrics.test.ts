import {
  SEGMENT_MIN_PX,
  coverageSummary,
  formatMonthYear,
  independenceSegments,
  paperByline,
  plural,
  splitBarColumns,
  splitBarSummary,
  surname,
} from './reuseMetrics';

describe('formatMonthYear', () => {
  it('shows month and year without shifting across time zones', () => {
    expect(formatMonthYear('2023-11-24')).toBe('Nov 2023');
    expect(formatMonthYear('2017-05')).toBe('May 2017');
    expect(formatMonthYear('2021-01-01T00:00:00')).toBe('Jan 2021');
  });
  it('keeps a bare year and passes through what it cannot read', () => {
    expect(formatMonthYear('2023')).toBe('2023');
    expect(formatMonthYear('soon')).toBe('soon');
    expect(formatMonthYear(null)).toBeNull();
    expect(formatMonthYear('')).toBeNull();
  });
});

describe('bylines', () => {
  it('reads surnames in either name order', () => {
    expect(surname('Sajjad Farashi')).toBe('Farashi');
    expect(surname('Cavanagh, James F')).toBe('Cavanagh');
    expect(surname('Madonna')).toBe('Madonna');
    expect(surname('   ')).toBeNull();
    expect(surname(null)).toBeNull();
  });
  it('drops invisible formatting characters OpenAlex sometimes carries', () => {
    expect(surname(`${String.fromCharCode(0x202c)}Siamak Shahidi`)).toBe('Shahidi');
    expect(surname(`Siamak Shahidi${String.fromCharCode(0x200b)}`)).toBe('Shahidi');
  });
  it('adds et al. only for several authors', () => {
    expect(paperByline('Sajjad Farashi', 5)).toBe('Farashi et al.');
    expect(paperByline('Sajjad Farashi', 1)).toBe('Farashi');
    expect(paperByline(null, 3)).toBeNull();
  });
  it('pluralises with separators', () => {
    expect(plural(1, 'reuse', 'reuses')).toBe('1 reuse');
    expect(plural(3, 'reuse', 'reuses')).toBe('3 reuses');
    expect(plural(2093, 'citing paper', 'citing papers')).toBe('2,093 citing papers');
  });
});

describe('splitBarColumns', () => {
  const ds003509 = [
    { year: 2021, reuse: 0, mentions: 3 },
    { year: 2022, reuse: 1, mentions: 2 },
    { year: 2023, reuse: 2, mentions: 2 },
    { year: 2024, reuse: 0, mentions: 1 },
    { year: 2025, reuse: 0, mentions: 3 },
    { year: 2026, reuse: 0, mentions: 0 },
  ];

  it('puts every year on one scale where the busiest fills the height', () => {
    const cols = splitBarColumns(ds003509, 80);
    expect(cols[2].reuseHeight + cols[2].mentionHeight).toBe(80);
    expect(cols[1].reuseHeight).toBe(20);
    expect(cols[1].mentionHeight).toBe(40);
    expect(cols[5].reuseHeight + cols[5].mentionHeight).toBe(0);
    expect(cols.map((c) => c.label)).toEqual(['2021', '2022', '2023', '2024', '2025', '2026']);
  });

  it('keeps a lone reuse visible next to a busy year, without a count that would not fit', () => {
    const [busy, quiet] = splitBarColumns([
      { year: 2015, reuse: 0, mentions: 60 },
      { year: 2016, reuse: 1, mentions: 0 },
    ], 80);
    expect(busy.showMentionCount).toBe(true);
    expect(quiet.reuseHeight).toBe(SEGMENT_MIN_PX);
    expect(quiet.showReuseCount).toBe(false);
  });

  it('shortens year labels when there are many years', () => {
    const years = Array.from({ length: 12 }, (_, i) => ({ year: 2015 + i, reuse: 0, mentions: 1 }));
    expect(splitBarColumns(years, 80)[0].label).toBe("'15");
  });

  it('handles no years and all-zero years', () => {
    expect(splitBarColumns([], 80)).toEqual([]);
    expect(splitBarColumns([{ year: 2024, reuse: 0, mentions: 0 }], 80)[0].reuseHeight).toBe(0);
  });

  it('describes only the years with papers', () => {
    expect(splitBarSummary(ds003509)).toBe(
      '2021: 0 reuses, 3 mentions; 2022: 1 reuse, 2 mentions; 2023: 2 reuses, 2 mentions; 2024: 0 reuses, 1 mention; 2025: 0 reuses, 3 mentions'
    );
    expect(splitBarSummary([{ year: 2026, reuse: 0, mentions: 0 }])).toBe('No reuse or mentions yet');
  });
});

describe('independenceSegments', () => {
  it('draws one segment per paper while they can be counted', () => {
    expect(independenceSegments(2, 1).map((s) => s.kind)).toEqual(['independent', 'independent', 'same_lab']);
  });
  it('switches to two proportional segments for many papers', () => {
    expect(independenceSegments(30, 10)).toEqual([
      { kind: 'independent', weight: 30 },
      { kind: 'same_lab', weight: 10 },
    ]);
    expect(independenceSegments(40, 0)).toEqual([{ kind: 'independent', weight: 40 }]);
  });
  it('has nothing to draw without reuse', () => {
    expect(independenceSegments(0, 0)).toEqual([]);
  });
});

describe('coverageSummary', () => {
  it('states what the numbers rest on', () => {
    expect(coverageSummary({ citing_papers: 20, classified: 14, no_full_text: 6, pending: 0 })).toEqual({
      text: '20 citing papers found: 14 classified, 6 without full text.',
      incomplete: false,
    });
  });
  it('flags papers still to classify', () => {
    expect(coverageSummary({ citing_papers: 2093, classified: 80, no_full_text: 392, pending: 1621 })).toEqual({
      text: '2,093 citing papers found: 80 classified, 392 without full text, 1,621 still to classify.',
      incomplete: true,
    });
  });
  it('says when nothing was found', () => {
    expect(coverageSummary({ citing_papers: 0, classified: 0, no_full_text: 0, pending: 0 }).text).toBe(
      'No citing papers found yet.'
    );
  });
});
