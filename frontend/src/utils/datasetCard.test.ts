import type { DatasetDetailResponse, TrackedDatasetMetrics } from '../services/api';
import type { DatasetPaperItem } from './datasetPapers';
import {
  caveats,
  citationText,
  contributorRoles,
  downloadCommand,
  histogram,
  labSplit,
  paperLab,
  rangeLabel,
  reuseTypeBreakdown,
  roleShares,
  takeaways,
} from './datasetCard';

const dataset: DatasetDetailResponse['dataset'] = {
  source: 'DANDI',
  dataset_id: '000070',
  title: 'Neural population dynamics during reaching',
  authors: ['Churchland, Mark', 'Cunningham, John P.'],
  contributors: [
    { name: 'Churchland, Mark', roles: ['dcite:Author', 'dcite:DataCollector'] },
    { name: 'Sharda, Saksham', roles: ['dcite:Maintainer'] },
  ],
  url: 'https://dandiarchive.org/dandiset/000070',
  created_at: '2021-05-05',
  num_subjects: 2,
};

const metrics: TrackedDatasetMetrics = {
  source: 'DANDI',
  dataset_id: '000070',
  tracked: true,
  published: '2021-05-05',
  reuse_count: 3,
  independent_reuse_count: 2,
  same_lab_reuse_count: 1,
  mention_count: 11,
  last_reuse: { doi: '10.1/x', first_author: 'Ali Farashi', author_count: 3, first_date: '2023-11-01', same_lab: false, same_lab_basis: [] },
  per_year: [
    { year: 2021, reuse: 0, mentions: 3 },
    { year: 2022, reuse: 1, mentions: 2 },
    { year: 2023, reuse: 2, mentions: 2 },
  ],
  undated: { reuse: 0, mentions: 0 },
  coverage: { citing_papers: 20, classified: 14, no_full_text: 6, pending: 0 },
  reuse_papers: [],
};

function paper(doi: string, label: DatasetPaperItem['label'], reuseType?: string, sameLab?: boolean): DatasetPaperItem {
  return {
    doi,
    label,
    title: doi,
    authors: [],
    journal: null,
    date: null,
    country: null,
    versions: [],
    isPreprint: false,
    citation: label === 'REUSE' ? ({ primary_paper_doi: 'p', citing_paper_doi: doi, classification: 'REUSE', reuse_type: reuseType, same_lab: sameLab } as any) : undefined,
  };
}

describe('citationText', () => {
  it('reads authors (year) title [Data set]. Archive. URL', () => {
    expect(citationText(dataset)).toBe(
      'Churchland, Mark; Cunningham, John P. (2021). Neural population dynamics during reaching [Data set]. DANDI Archive. https://dandiarchive.org/dandiset/000070',
    );
  });
  it('copes without authors or a date', () => {
    expect(citationText({ ...dataset, authors: null, created_at: null })).toBe(
      'Neural population dynamics during reaching [Data set]. DANDI Archive. https://dandiarchive.org/dandiset/000070',
    );
  });
});

describe('downloadCommand', () => {
  it('knows the DANDI and OpenNeuro clients and nothing else', () => {
    expect(downloadCommand('DANDI', '000070')).toBe('dandi download DANDI:000070');
    expect(downloadCommand('OpenNeuro', 'ds000001')).toBe('openneuro-py download --dataset ds000001');
    expect(downloadCommand('CRCNS', 'pvc-1')).toBeNull();
  });
});

describe('contributorRoles', () => {
  it('groups non-author roles by role, with the dcite prefix dropped and words split', () => {
    expect(contributorRoles(dataset.contributors)).toEqual([
      { role: 'Data Collector', names: ['Churchland, Mark'] },
      { role: 'Maintainer', names: ['Sharda, Saksham'] },
    ]);
  });
  it('lists each name once per role and keeps every role', () => {
    const roles = contributorRoles([
      { name: 'Simons Foundation', roles: ['dcite:Funder', 'dcite:Sponsor', 'dcite:Funder'] },
      { name: 'EPSRC', roles: ['dcite:Funder'] },
      { name: 'A', roles: ['dcite:ContactPerson'] },
      { name: 'B', roles: ['dcite:DataCurator'] },
      { name: 'C', roles: ['dcite:Methodology'] },
      { name: 'D', roles: ['dcite:Software'] },
    ]);
    expect(roles).toEqual([
      { role: 'Funder', names: ['Simons Foundation', 'EPSRC'] },
      { role: 'Sponsor', names: ['Simons Foundation'] },
      { role: 'Contact Person', names: ['A'] },
      { role: 'Data Curator', names: ['B'] },
      { role: 'Methodology', names: ['C'] },
      { role: 'Software', names: ['D'] },
    ]);
  });
});

describe('reuseTypeBreakdown', () => {
  it('counts reuse papers by type, split by lab, biggest first', () => {
    const rows = reuseTypeBreakdown(
      [paper('a', 'REUSE', 'TOOL_DEMO', false), paper('b', 'REUSE', 'TOOL_DEMO', true), paper('c', 'REUSE', 'BENCHMARK'), paper('d', 'MENTION')],
      new Map([['c', false]]),
    );
    expect(rows).toEqual([
      // 'a' has no metrics call and the classifier only said "not same lab": unknown, not independent.
      { key: 'TOOL_DEMO', label: 'Tool demo', independent: 0, sameLab: 1, unknown: 1 },
      { key: 'BENCHMARK', label: 'Benchmark', independent: 1, sameLab: 0, unknown: 0 },
    ]);
  });
  it('prefers the metrics same-lab call over the classifier', () => {
    const rows = reuseTypeBreakdown([paper('a', 'REUSE', 'SIMULATION', false)], new Map([['a', true]]));
    expect(rows[0]).toMatchObject({ independent: 0, sameLab: 1, unknown: 0 });
  });
  it('agrees with the paper list before /metrics answers', () => {
    const papers = [paper('a', 'REUSE', 'SIMULATION', false), paper('b', 'REUSE', 'SIMULATION')];
    const [row] = reuseTypeBreakdown(papers, new Map());
    const split = labSplit(papers, new Map());
    expect({ independent: row.independent, sameLab: row.sameLab, unknown: row.unknown }).toEqual(split);
  });
});

describe('takeaways', () => {
  it('adds first reuse and the mention majority, not what the Impact tiles already show', () => {
    expect(takeaways(dataset, metrics)).toEqual([
      'First reuse came in 2022, 1 year after release.',
      'Most classified citing papers mention it without reusing it: 11 of 14.',
    ]);
  });
  it('only says "most" for more than half of the classified papers', () => {
    // 6 mentions and 3 reuses of 14 classified: neither is a majority.
    const split = { ...metrics, mention_count: 6, coverage: { ...metrics.coverage, classified: 14 } };
    expect(takeaways(dataset, split)).toContain('Of 14 classified citing papers, 3 reused the data and 6 mentioned it.');
    const reuseHeavy = { ...metrics, reuse_count: 9, mention_count: 2 };
    expect(takeaways(dataset, reuseHeavy)).toContain('Most classified citing papers reused the data: 9 of 14.');
  });
  it('says so when nothing has been reused', () => {
    const none = { ...metrics, reuse_count: 0, independent_reuse_count: 0, same_lab_reuse_count: 0, last_reuse: null, per_year: [] };
    expect(takeaways(dataset, none)).toEqual(['No reuse found yet among 14 classified citing papers.']);
  });
  it('is empty without metrics', () => {
    expect(takeaways(dataset, null)).toEqual([]);
  });
});

describe('caveats', () => {
  it('names the coverage gap and the fixed notes', () => {
    const notes = caveats(dataset, metrics, true, 20);
    expect(notes[0]).toBe('Impact counts are lower bounds: of 20 citing papers, 6 had no full text to classify.');
    expect(notes[notes.length - 1]).toBe('Citing papers come from OpenAlex, which indexes most but not all of the literature.');
    expect(notes.some((n) => /subject/.test(n))).toBe(false);
  });
  it('explains untracked archives and junk status first', () => {
    const notes = caveats({ ...dataset, source: 'CRCNS', dataset_status: 'excluded' }, null, false, 0);
    expect(notes[0]).toMatch(/^Flagged as a test/);
    expect(notes[1]).toMatch(/not tracked for CRCNS/);
  });
});

describe('histogram and rangeLabel', () => {
  it('bins values between min and max', () => {
    expect(histogram([1, 2, 3, 10], 3)).toEqual([3, 0, 1]);
    expect(histogram([], 3)).toEqual([]);
    expect(histogram([5, 5], 3)).toEqual([2, 0, 0]);
  });
  it('labels the range', () => {
    expect(rangeLabel([2013, 2018, 2015])).toBe('2013 to 2018');
    expect(rangeLabel([2013])).toBe('2013');
    expect(rangeLabel([])).toBe('—');
  });
});

describe('roleShares', () => {
  it('splits by label in the chip order and leaves empty labels out', () => {
    const shares = roleShares([paper('a', 'MENTION'), paper('b', 'PRIMARY'), paper('c', 'MENTION'), paper('d', 'MENTION')]);
    expect(shares).toEqual([
      { label: 'PRIMARY', count: 1, share: 0.25 },
      { label: 'MENTION', count: 3, share: 0.75 },
    ]);
  });
});

describe('paperLab and labSplit', () => {
  const papers = [paper('a', 'REUSE', 'X', false), paper('b', 'REUSE', 'X', true), paper('c', 'REUSE'), paper('d', 'MENTION')];
  it("trusts the metrics' call first, then only the classifier's same lab", () => {
    const metricsCall = new Map([['a', false]]);
    expect(paperLab(papers[0], metricsCall)).toBe(false);
    expect(paperLab(papers[1], metricsCall)).toBe(true);
    // The classifier's "not same lab" alone isn't enough to call it independent.
    expect(paperLab(papers[0], new Map())).toBeUndefined();
    expect(paperLab(papers[3], metricsCall)).toBeUndefined();
  });
  it('counts reuse papers by lab, unknowns apart', () => {
    expect(labSplit(papers, new Map([['a', false], ['c', false]]))).toEqual({ independent: 2, sameLab: 1, unknown: 0 });
    expect(labSplit(papers, new Map())).toEqual({ independent: 0, sameLab: 1, unknown: 2 });
  });
});
