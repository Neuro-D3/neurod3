// Mock NeuroD3 API on port 8000, with one fully populated DANDI dataset
// (000070, the SfN poster's), one untracked CRCNS dataset, and paper-mapping
// summaries. Lets the frontend run without Postgres and Airflow:
//
//   npm run mock      (this file)
//   npm start         (in another terminal; the app talks to localhost:8000)
//
// Then open http://localhost:3000, /datasets/dandi/000070 and /paper-mapping.
const http = require('http');

const DS = {
  source: 'DANDI',
  dataset_id: '000070',
  title: 'Neural population dynamics during reaching',
  description:
    'Spiking activity recorded from primary motor cortex (M1) and dorsal premotor cortex (PMd) of two monkeys reaching with the right hand.',
  full_description:
    'Spiking activity recorded from primary motor cortex (M1) and dorsal premotor cortex (PMd) of two monkeys reaching with the right hand, on two 96-channel high-density Utah arrays from Blackrock Microsystems.\n\nSuited to population dynamics analyses of reaching, latent variable models of single-trial dynamics such as LFADS, and decoding studies of movement preparation and timing.',
  authors: ['Churchland, Mark', 'Cunningham, John P.', 'Kaufman, Matthew T.', 'Foster, Justin D.', 'Nuyujukian, Paul', 'Ryu, Stephen I.', 'Shenoy, Krishna V.'],
  contributors: [
    { name: 'Churchland, Mark', roles: ['dcite:Author', 'dcite:DataCollector', 'dcite:ContactPerson'] },
    { name: 'Cunningham, John P.', roles: ['dcite:Author', 'dcite:DataCollector'] },
    { name: 'Sharda, Saksham', roles: ['dcite:Maintainer'] },
    { name: 'Adkisson-Floro, Paul', roles: ['dcite:DataCurator'] },
    { name: 'Simons Foundation', roles: ['dcite:Funder'] },
    { name: 'EPSRC', roles: ['dcite:Funder'] },
  ],
  license: 'spdx:CC-BY-4.0',
  num_subjects: 2,
  modality: 'Extracellular electrophysiology; Behavior',
  papers: 1,
  url: 'https://dandiarchive.org/dandiset/000070',
  created_at: '2021-05-05',
  updated_at: '2026-05-28T10:00:00Z',
  dataset_status: 'included',
};

const PRIMARY = [
  {
    paper_doi: '10.1038/nature11129',
    paper_title: 'Neural population dynamics during reaching',
    authors: ['Mark M. Churchland', 'John P. Cunningham', 'Matthew T. Kaufman', 'Krishna V. Shenoy'],
    openalex_id: 'W2095563612',
    journal: 'Nature',
    senior_author_country: 'US',
    publication_date: '2012-06-03',
    publication_year: 2012,
    citing_papers_count: 20,
    citing_works_count: 20,
    work_versions: [],
  },
];

function cit(doi, title, cls, year, extra = {}) {
  return {
    primary_paper_doi: '10.1038/nature11129',
    citing_paper_doi: doi,
    citing_paper_title: title,
    citing_authors: extra.authors || ['A. Author', 'B. Author', 'C. Author', 'D. Author'],
    citing_journal: extra.journal || 'Journal of Neuroscience',
    citing_senior_author_country: extra.country || 'US',
    citing_publication_date: `${year}-${extra.month || '06'}-01`,
    citing_publication_year: year,
    classification: cls,
    classification_status: cls,
    confidence: extra.confidence ?? 8,
    reasoning: extra.reasoning || 'The methods section states that the publicly available reaching dataset was downloaded from DANDI and used for model fitting.',
    reuse_type: extra.reuse_type,
    reused_modalities: extra.modalities,
    same_lab: extra.same_lab,
    source_archive: cls === 'REUSE' ? 'DANDI' : undefined,
    evidence_quotes: cls === 'REUSE' ? [{ quote: 'We used the publicly available motor cortex recordings (DANDI:000070) to fit the model.', match_type: 'exact' }] : [],
    hallucinated_quote_count: 0,
    citing_text_status: 'full_text',
  };
}

const CITATIONS = [
  cit('10.1038/s41592-018-0109-9', 'Inferring single-trial neural population dynamics using sequential auto-encoders', 'REUSE', 2022, { journal: 'Nature Methods', reuse_type: 'TOOL_DEMO', modalities: ['neurophysiology'], same_lab: true, authors: ['Chethan Pandarinath', 'Daniel J. O’Shea', 'Jasmine Collins'] }),
  cit('10.1186/s12883-023-03468-0', 'Vacillation, indecision and hesitation in moment-by-moment decoding of monkey motor cortex', 'REUSE', 2023, { journal: 'eLife', reuse_type: 'NOVEL_ANALYSIS', modalities: ['neurophysiology', 'behavior'], same_lab: false, month: '11', authors: ['Ali Farashi', 'M. Kiani', 'S. Rezaei'] }),
  cit('10.1523/eneuro.0085-16.2016', 'The largest response component in the motor cortex reflects movement timing but not movement type', 'REUSE', 2023, { journal: 'eNeuro', reuse_type: 'BENCHMARK', modalities: ['neurophysiology'], same_lab: false, month: '03' }),
  cit('10.1038/nn.4042', 'A neural network that finds a naturalistic solution for the production of muscle activity', 'MENTION', 2021, { journal: 'Nature Neuroscience' }),
  cit('10.1038/nn.3643', 'Cortical activity in the null space: permitting preparation without movement', 'MENTION', 2021, { journal: 'Nature Neuroscience' }),
  cit('10.1152/jn.00892.2012', 'The roles of monkey M1 neuron classes in movement preparation and execution', 'MENTION', 2021, { journal: 'Journal of Neurophysiology' }),
  cit('10.1146/annurev-neuro-062111-150509', 'Cortical control of arm movements: a dynamical systems perspective', 'MENTION', 2022, { journal: 'Annual Review of Neuroscience' }),
  cit('10.1101/2022.01.01.000001', 'Latent dynamics of motor cortex during delayed reaching', 'MENTION', 2022, { journal: 'bioRxiv' }),
  cit('10.1016/j.neuron.2023.01.001', 'Preparatory activity and the expansive null-space', 'MENTION', 2023, { journal: 'Neuron' }),
  cit('10.1016/j.neuron.2023.02.002', 'Rotational dynamics in motor cortex are consistent with a feedback controller', 'MENTION', 2023, { journal: 'Neuron' }),
  cit('10.1038/s41593-024-0001-1', 'A unifying perspective on neural manifolds and circuits for cognition', 'MENTION', 2024, { journal: 'Nature Neuroscience' }),
  cit('10.1038/s41586-025-0001-1', 'Flexible neural control of motor units', 'MENTION', 2025, { journal: 'Nature' }),
  cit('10.1016/j.cub.2025.0001', 'Motor cortical dynamics are shaped by multiple distinct subspaces', 'MENTION', 2025, { journal: 'Current Biology' }),
  cit('10.7554/elife.2025.0001', 'Neural population dynamics in motor cortex are different for reach and grasp', 'MENTION', 2025, { journal: 'eLife' }),
  cit('10.1101/2024.05.05.000001', 'Untitled without full text', 'no_full_text', 2024, {}),
];
CITATIONS[CITATIONS.length - 1].classification = null;
CITATIONS[CITATIONS.length - 1].classification_status = 'no_full_text';

const METRICS = {
  source: 'DANDI',
  dataset_id: '000070',
  tracked: true,
  published: '2021-05-05',
  reuse_count: 3,
  independent_reuse_count: 2,
  same_lab_reuse_count: 1,
  mention_count: 11,
  last_reuse: { doi: '10.1186/s12883-023-03468-0', title: 'Vacillation, indecision and hesitation', first_author: 'Ali Farashi', author_count: 3, publication_date: '2023-11-01', first_date: '2023-11-01', same_lab: false, same_lab_basis: [] },
  per_year: [
    { year: 2021, reuse: 0, mentions: 3 },
    { year: 2022, reuse: 1, mentions: 2 },
    { year: 2023, reuse: 2, mentions: 2 },
    { year: 2024, reuse: 0, mentions: 1 },
    { year: 2025, reuse: 0, mentions: 3 },
    { year: 2026, reuse: 0, mentions: 0 },
  ],
  undated: { reuse: 0, mentions: 0 },
  coverage: { citing_papers: 20, classified: 14, no_full_text: 6, pending: 0 },
  reuse_papers: [
    { doi: '10.1038/s41592-018-0109-9', same_lab: true, same_lab_basis: ['author_names'], author_count: 3 },
    { doi: '10.1186/s12883-023-03468-0', same_lab: false, same_lab_basis: [], author_count: 3 },
    { doi: '10.1523/eneuro.0085-16.2016', same_lab: false, same_lab_basis: [], author_count: 4 },
  ],
};

const LIST = [
  { source: 'DANDI', id: '000070', title: DS.title, modality: DS.modality, papers: 1, reuse_count: 3, paper_dois: ['10.1038/nature11129'], paper_titles: ['Neural population dynamics during reaching'], url: DS.url, description: DS.description, authors: DS.authors, num_subjects: 2, created_at: '2021-05-05', updated_at: '2026-05-28' },
  { source: 'OpenNeuro', id: 'ds000117', title: 'A multi-subject, multi-modal human neuroimaging dataset', modality: 'MRI; MEG; EEG', papers: 2, reuse_count: 41, paper_dois: ['10.1038/sdata.2015.1'], paper_titles: ['A multi-subject, multi-modal human neuroimaging dataset'], url: 'https://openneuro.org/datasets/ds000117', description: 'Simultaneous MEG/EEG and fMRI from 16 participants viewing famous, unfamiliar and scrambled faces.', authors: ['Wakeman, Daniel G.', 'Henson, Richard N.'], num_subjects: 16, created_at: '2015-01-20', updated_at: '2024-02-10' },
  { source: 'CRCNS', id: 'pvc-1', title: 'Single and multi-unit recordings from primary visual cortex of anesthetized macaque', modality: 'Extracellular electrophysiology', papers: 1, reuse_count: 7, url: 'https://crcns.org/data-sets/vc/pvc-1', description: 'Responses of V1 neurons to natural movies and gratings.', authors: ['Ringach, Dario', 'Nauhaus, Ian'], num_subjects: 3, created_at: '2008', created_at_precision: 'year' },
  { source: 'SPARC', id: '65', title: 'Vagus nerve morphology in the pig', modality: 'Microscopy', papers: 1, reuse_count: 0, url: 'https://sparc.science/datasets/65', description: 'Fascicle-level anatomy of the cervical vagus nerve.', authors: ['Vagus Nerve Group, University of Wisconsin'], num_subjects: 8, created_at: '2020-09-14', updated_at: '2023-01-01' },
  { source: 'DANDI', id: '000006', title: 'Mouse anterior lateral motor cortex (ALM) in delay response task', modality: 'Extracellular electrophysiology', papers: 1, reuse_count: 2, url: 'https://dandiarchive.org/dandiset/000006', description: 'Extracellular recordings from ALM during a tactile delayed-response task.', authors: ['Economo, Michael', 'Svoboda, Karel'], num_subjects: 12, created_at: '2020-03-05', updated_at: '2022-10-01' },
];

const SUMMARY = {
  summary: { ingested_total: 1204, junk_datasets: 36, ingested_datasets: 1168, no_paper_datasets: 410, pending_datasets: 120, datasets_with_mapped_papers: 638, distinct_mapped_primary_papers: 712, citation_edges: 15038, citations_with_contexts: 9021, citation_context_count: 20311, classified_edges: 297, placeholder_classification_edges: 14741 },
  by_source: [
    { source: 'DANDI', ingested_total: 420, junk_datasets: 30, ingested_datasets: 390, no_paper_datasets: 110, pending_datasets: 20, never_published_datasets: 60, junk_reasons: { empty: 20, placeholder_title: 10 }, datasets_with_mapped_papers: 260, distinct_mapped_primary_papers: 300, citation_edges: 8000, citations_with_contexts: 5000, classified_edges: 200 },
    { source: 'OpenNeuro', ingested_total: 700, junk_datasets: 6, ingested_datasets: 694, no_paper_datasets: 290, pending_datasets: 100, datasets_with_mapped_papers: 304, distinct_mapped_primary_papers: 350, citation_edges: 6500, citations_with_contexts: 3800, classified_edges: 90 },
  ],
  by_classification: { REUSE: 3, MENTION: 150, NEITHER: 120, no_full_text: 20, error: 4, placeholder: 14741 },
};

const PM_ROWS = LIST.map((d, i) => ({ source: d.source, dataset_id: d.id, dataset_title: d.title, dataset_description: d.description, modality: d.modality, papers_count: d.papers, url: d.url, mapped_papers_count: d.papers, citation_edges_count: [20, 300, 40, 2, 12][i], citations_with_contexts_count: [14, 200, 20, 0, 9][i], contexts_extracted_count: [14, 200, 20, 0, 9][i], latest_citing_publication_date: '2025-08-01', classified_edges_count: 14, placeholder_classification_edges_count: 6 }));

function json(res, body) {
  res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' });
  res.end(JSON.stringify(body));
}

http
  .createServer((req, res) => {
    const u = new URL(req.url, 'http://localhost');
    const p = u.pathname;
    if (p === '/api/datasets/stats') return json(res, { total: 1168, by_source: { DANDI: 390, OpenNeuro: 694, CRCNS: 72, SPARC: 12 }, by_modality: { 'Extracellular electrophysiology': 400, MRI: 500, EEG: 120, Behavior: 300, MEG: 30, Microscopy: 10 } });
    if (p === '/api/datasets') {
      const src = u.searchParams.get('source');
      const rows = src ? LIST.filter((d) => d.source === src) : LIST;
      return json(res, { datasets: rows, count: src ? rows.length : 1168 });
    }
    if (p === '/api/datasets/dandi/000070') return json(res, { dataset: DS, primary_papers: PRIMARY, citations: CITATIONS });
    if (p === '/api/datasets/dandi/000070/metrics') return json(res, METRICS);
    if (p === '/api/datasets/crcns/pvc-1') return json(res, { dataset: { ...DS, source: 'CRCNS', dataset_id: 'pvc-1', title: LIST[2].title, authors: LIST[2].authors, contributors: null, license: null, num_subjects: 3, modality: LIST[2].modality, url: LIST[2].url, created_at: '2008', created_at_precision: 'year', full_description: LIST[2].description }, primary_papers: [], citations: [] });
    if (p === '/api/datasets/crcns/pvc-1/metrics') return json(res, { source: 'CRCNS', dataset_id: 'pvc-1', tracked: false });
    if (p === '/api/paper-mapping/summary') return json(res, SUMMARY);
    if (p === '/api/paper-mapping/datasets') return json(res, { datasets: PM_ROWS, count: PM_ROWS.length });
    if (p.startsWith('/api/paper-mapping/datasets/')) return json(res, { dataset: PM_ROWS[0], primary_papers: PRIMARY.map((x) => ({ ...x, citations_with_contexts_count: 14, classified_edges_count: 14, placeholder_classification_edges_count: 6 })), citations: CITATIONS });
    if (p === '/api/paper-mapping/citations') return json(res, { citations: CITATIONS.map((c) => ({ ...c, source: 'DANDI', dataset_id: '000070', citation_contexts: [{ context: 'as in the reaching dataset of Churchland et al. (2012)' }] })), count: CITATIONS.length });
    res.writeHead(404, { 'Access-Control-Allow-Origin': '*' });
    res.end('not found');
  })
  .listen(8000, () => console.log('mock api on 8000'));

