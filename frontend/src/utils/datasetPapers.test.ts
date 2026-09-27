import type { DatasetDetailCitation, DatasetDetailPaper } from '../services/api';
import { buildPaperList, countByLabel } from './datasetPapers';

function primary(doi: string, date: string, extra: Partial<DatasetDetailPaper> = {}): DatasetDetailPaper {
  return { paper_doi: doi, paper_title: `Primary ${doi}`, publication_date: date, citing_papers_count: 10, ...extra };
}

function citing(
  doi: string,
  classification: string | null,
  date: string | null,
  extra: Partial<DatasetDetailCitation> = {},
): DatasetDetailCitation {
  return {
    primary_paper_doi: '10.1/p1',
    citing_paper_doi: doi,
    citing_paper_title: `Citing ${doi}`,
    citing_publication_date: date,
    classification,
    ...extra,
  };
}

describe('buildPaperList', () => {
  it('lists primary papers, then reuse, then mentions, newest first within each', () => {
    const items = buildPaperList(
      [primary('10.1/p1', '2014-11-04'), primary('10.1/p2', '2017-05')],
      [
        citing('10.1/m-old', 'MENTION', '2021-06-10'),
        citing('10.1/r-old', 'REUSE', '2022-12-14'),
        citing('10.1/m-new', 'MENTION', '2025-11-01'),
        citing('10.1/r-new', 'REUSE', '2023-11-24'),
      ],
    );
    expect(items.map((i) => [i.label, i.doi])).toEqual([
      ['PRIMARY', '10.1/p2'],
      ['PRIMARY', '10.1/p1'],
      ['REUSE', '10.1/r-new'],
      ['REUSE', '10.1/r-old'],
      ['MENTION', '10.1/m-new'],
      ['MENTION', '10.1/m-old'],
    ]);
  });

  it('lists a citing paper once, under its strongest label across primary papers', () => {
    const items = buildPaperList([], [
      citing('10.1/a', 'MENTION', '2023-01-01', { primary_paper_doi: '10.1/p1' }),
      citing('10.1/a', 'REUSE', '2023-01-01', { primary_paper_doi: '10.1/p2', reuse_type: 'ML_TRAINING' }),
      citing('10.1/b', 'NEITHER', '2023-01-01', { primary_paper_doi: '10.1/p1' }),
      citing('10.1/b', 'MENTION', '2023-01-01', { primary_paper_doi: '10.1/p2' }),
    ]);
    expect(items.map((i) => [i.doi, i.label])).toEqual([['10.1/a', 'REUSE'], ['10.1/b', 'MENTION']]);
    expect(items[0].citation?.reuse_type).toBe('ML_TRAINING');
  });

  it('leaves out neither, unclassified and failed citing papers', () => {
    const items = buildPaperList([], [
      citing('10.1/n', 'NEITHER', null),
      citing('10.1/u', null, null),
      citing('10.1/e', null, null, { status: 'error' }),
    ]);
    expect(items).toEqual([]);
  });

  it('does not repeat a primary paper that also turns up as a citing paper', () => {
    const items = buildPaperList([primary('10.1/p1', '2017-05')], [citing('10.1/p1', 'MENTION', '2017-05')]);
    expect(items.map((i) => [i.doi, i.label])).toEqual([['10.1/p1', 'PRIMARY']]);
  });

  it('keeps what the list rows show', () => {
    const [p, r] = buildPaperList(
      [primary('10.1/p1', '', { publication_date: null, publication_year: 2014, openalex_id: 'W1', citing_papers_count: 7 })],
      [citing('10.1/r', 'reuse', '2023-11-24', { citing_authors: ['Sajjad Farashi'], citing_journal: 'BMC Neurol' })],
    );
    expect([p.date, p.citingCount, p.openalexId]).toEqual(['2014', 7, 'W1']);
    expect([r.label, r.authors, r.journal]).toEqual(['REUSE', ['Sajjad Farashi'], 'BMC Neurol']);
  });

  it('lists a preprint and its published version once, as the published one', () => {
    const versions = [
      { doi: '10.1101/2023.05.08.539865', is_preprint: true, publication_date: '2023-05-10' },
      { doi: '10.1111/psyp.14478', is_preprint: false, publication_date: '2023-11-08' },
    ];
    const shared = { citing_work_key: 't:medication', citing_work_doi: '10.1111/psyp.14478', citing_work_versions: versions };
    const items = buildPaperList([], [
      citing('10.1101/2023.05.08.539865', 'REUSE', '2023-05-10', { ...shared, citing_is_preprint: true, reuse_type: 'NOVEL_ANALYSIS' }),
      citing('10.1111/psyp.14478', null, '2023-11-08', { ...shared, citing_is_preprint: false, citing_journal: 'Psychophysiology' }),
    ]);
    expect(items).toHaveLength(1);
    const [paper] = items;
    // Shown as the published version, labelled and explained by the version that was classified.
    expect([paper.doi, paper.label, paper.journal, paper.date, paper.isPreprint]).toEqual(
      ['10.1111/psyp.14478', 'REUSE', 'Psychophysiology', '2023-11-08', false]);
    expect(paper.citation?.reuse_type).toBe('NOVEL_ANALYSIS');
    expect(paper.versions.map((v) => v.doi)).toEqual(['10.1101/2023.05.08.539865', '10.1111/psyp.14478']);
  });

  it('marks a paper only known as a preprint', () => {
    const [paper] = buildPaperList([], [citing('10.1101/430858', 'MENTION', '2018', { citing_is_preprint: true })]);
    expect([paper.label, paper.isPreprint, paper.versions]).toEqual(['MENTION', true, []]);
  });

  it("groups primary papers by work too, showing the API's count of citing works", () => {
    // 3 + 7 citing DOIs, but papers citing both versions are one citing work.
    const items = buildPaperList([
      primary('10.1101/2020.01.01.111111', '2020-01', {
        work_key: 't:data', work_doi: '10.1038/data', citing_papers_count: 3, citing_works_count: 8, is_preprint: true,
      }),
      primary('10.1038/data', '2020-06', { work_key: 't:data', work_doi: '10.1038/data', citing_papers_count: 7, citing_works_count: 8 }),
    ], []);
    expect(items.map((i) => [i.doi, i.citingCount])).toEqual([['10.1038/data', 8]]);
  });

  it('leaves out a citing paper that is a version of a primary paper', () => {
    const items = buildPaperList(
      [primary('10.1038/data', '2020-06', { work_key: 't:data' })],
      [citing('10.1101/2020.01.01.111111', 'MENTION', '2020-01', { citing_work_key: 't:data' })],
    );
    expect(items.map((i) => i.label)).toEqual(['PRIMARY']);
  });

  it('counts by label', () => {
    const items = buildPaperList([primary('10.1/p1', '2017')], [citing('10.1/r', 'REUSE', null), citing('10.1/m', 'MENTION', null)]);
    expect(countByLabel(items)).toEqual({ PRIMARY: 1, REUSE: 1, MENTION: 1 });
  });
});
