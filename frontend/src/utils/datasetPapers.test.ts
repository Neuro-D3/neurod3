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

  it('counts by label', () => {
    const items = buildPaperList([primary('10.1/p1', '2017')], [citing('10.1/r', 'REUSE', null), citing('10.1/m', 'MENTION', null)]);
    expect(countByLabel(items)).toEqual({ PRIMARY: 1, REUSE: 1, MENTION: 1 });
  });
});
