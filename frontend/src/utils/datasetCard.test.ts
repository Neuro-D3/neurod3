import type { DatasetDetailResponse, TrackedDatasetMetrics } from '../services/api';
import type { DatasetPaperItem } from './datasetPapers';
import {
  caveats,
  citationText,
  contributorRoles,
  downloadCommand,
  histogram,
  rangeLabel,
  reuseTypeBreakdown,
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
});

describe('reuseTypeBreakdown', () => {
  it('counts reuse papers by type, split by lab, biggest first', () => {
    const rows = reuseTypeBreakdown(
      [paper('a', 'REUSE', 'TOOL_DEMO', false), paper('b', 'REUSE', 'TOOL_DEMO', true), paper('c', 'REUSE', 'BENCHMARK'), paper('d', 'MENTION')],
      new Map([['c', false]]),
    );
    expect(rows).toEqual([
      { key: 'TOOL_DEMO', label: 'Tool demo', independent: 1, sameLab: 1 },
      { key: 'BENCHMARK', label: 'Benchmark', independent: 1, sameLab: 0 },
    ]);
  });
  it('prefers the metrics same-lab call over the classifier', () => {
    const rows = reuseTypeBreakdown([paper('a', 'REUSE', 'SIMULATION', false)], new Map([['a', true]]));
    expect(rows[0]).toMatchObject({ independent: 0, sameLab: 1 });
  });
});

describe('takeaways', () => {
  it('derives lab split, first reuse, mention share and the latest reuse', () => {
    expect(takeaways(dataset, metrics, [])).toEqual([
      '2 different-lab reuse papers and 1 from the dataset’s own lab.',
      'First reuse came in 2022, 1 year after release.',
      'Most citing papers mention it without reusing it: 11 of 14 classified.',
      'Most recently reused by Farashi et al. in 2023.',
    ]);
  });
  it('says so when nothing has been reused', () => {
    const none = { ...metrics, reuse_count: 0, independent_reuse_count: 0, same_lab_reuse_count: 0, last_reuse: null, per_year: [] };
    expect(takeaways(dataset, none, [])).toContain('No reuse found yet among 14 classified citing papers.');
  });
  it('is empty without metrics', () => {
    expect(takeaways(dataset, null, [])).toEqual([]);
  });
});

describe('caveats', () => {
  it('names the coverage gap and the fixed notes', () => {
    const notes = caveats(dataset, metrics, true, 20);
    expect(notes[0]).toBe('Impact counts are lower bounds: of 20 citing papers, 6 had no full text to classify.');
    expect(notes).toContain('Citing papers come from OpenAlex, which indexes most but not all of the literature.');
    expect(notes[notes.length - 1]).toBe('2 subjects limit cross-subject generalization.');
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
    expect(rangeLabel([66, 97, 70], 'characters')).toBe('66 to 97 characters');
    expect(rangeLabel([2013])).toBe('2013');
    expect(rangeLabel([])).toBe('—');
  });
});
