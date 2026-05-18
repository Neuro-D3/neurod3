/**
 * API service for fetching neuroscience datasets from the backend.
 */

function inferApiBaseUrl(): string {
  // Preferred: explicit env var (CI / custom deployments)
  const envUrl = process.env.REACT_APP_API_URL;
  if (envUrl && envUrl.trim()) return envUrl.replace(/\/+$/, '');

  // PR preview default: derive `/pr-<N>/api` from current pathname.
  // Example: `/pr-45/app/` -> `/pr-45/api`
  if (typeof window !== 'undefined') {
    const m = window.location.pathname.match(/^(\/pr-\d+)(?=\/)/);
    if (m?.[1]) return `${m[1]}/api`;
  }

  // Local dev fallback (direct API port)
  return 'http://localhost:8000';
}

const API_BASE_URL = inferApiBaseUrl();

export interface Dataset {
  source: 'CRCNS' | 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet' | 'SPARC';
  id: string;
  title: string;
  modality: string | null;
  tags?: string | null;
  papers: number | null;
  paper_dois?: string[] | null;
  paper_titles?: string[] | null;
  /** Number of distinct citing papers classified as SECONDARY reuse by the LLM. */
  secondary_reuse_count?: number | null;
  url: string;
  description?: string;
  authors?: string[] | null;
  num_subjects?: number | null;
  created_at?: string;
  updated_at?: string;
}

export interface DatasetsResponse {
  datasets: Dataset[];
  count: number;
}

export interface DatasetStats {
  total: number;
  by_source: Record<string, number>;
  by_modality: Record<string, number>;
}

export interface ApiHealthResponse {
  status: string;
  database: string;
}

export interface PaperMappingSourceSummary {
  source: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';
  datasets_with_mapped_papers: number;
  distinct_mapped_primary_papers: number;
  citation_edges: number;
  citations_with_contexts: number;
  classified_edges: number;
}

export interface PaperMappingSummary {
  summary: {
    datasets_with_mapped_papers: number;
    distinct_mapped_primary_papers: number;
    citation_edges: number;
    citations_with_contexts: number;
    citation_context_count: number;
    classified_edges: number;
    placeholder_classification_edges: number;
  };
  by_source: PaperMappingSourceSummary[];
  by_classification: Record<string, number>;
}

export interface PaperMappingDatasetRow {
  source: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';
  dataset_id: string;
  dataset_title: string | null;
  dataset_description?: string | null;
  modality?: string | null;
  papers_count?: number | null;
  created_at?: string | null;
  updated_at?: string | null;
  url?: string | null;
  mapped_papers_count: number;
  citation_edges_count: number;
  citations_with_contexts_count: number;
  contexts_extracted_count: number;
  latest_primary_publication_date?: string | null;
  latest_citing_publication_date?: string | null;
  classified_edges_count: number;
  placeholder_classification_edges_count: number;
}

export interface PaperMappingDatasetsResponse {
  datasets: PaperMappingDatasetRow[];
  count: number;
}

export interface PaperMappingPrimaryPaper {
  paper_doi: string;
  doi_source?: string | null;
  relation_type?: string | null;
  resolved_at?: string | null;
  run_id?: string | null;
  paper_title?: string | null;
  authors?: string[] | null;
  openalex_id?: string | null;
  publication_date?: string | null;
  publication_year?: number | null;
  citing_papers_count: number;
  citations_with_contexts_count: number;
  classified_edges_count: number;
  placeholder_classification_edges_count: number;
}

export interface PaperMappingCitation {
  source?: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';
  dataset_id?: string;
  primary_paper_doi: string;
  primary_paper_title?: string | null;
  citing_paper_doi: string;
  citing_paper_title?: string | null;
  citing_authors?: string[] | null;
  citing_publication_date?: string | null;
  citing_publication_date_from_papers?: string | null;
  citing_publication_year?: number | null;
  citation_source?: string | null;
  matched_primary_paper_doi?: string | null;
  matched_primary_openalex_id?: string | null;
  citation_contexts?: Array<{ context?: string; method?: string; [key: string]: any }> | null;
  contexts_extracted_at?: string | null;
  classification_status?: string | null;
  classification?: string | null;
  same_lab?: boolean | null;
  confidence?: number | null;
  status?: string | null;
  reasoning?: string | null;
  classification_model?: string | null;
  classified_at?: string | null;
}

export interface PaperMappingDatasetDetail {
  dataset: PaperMappingDatasetRow;
  primary_papers: PaperMappingPrimaryPaper[];
  citations: PaperMappingCitation[];
}

export interface PaperMappingCitationsResponse {
  citations: PaperMappingCitation[];
  count: number;
}

export interface DatasetDetailPaper {
  paper_doi: string;
  doi_source?: string | null;
  relation_type?: string | null;
  paper_title?: string | null;
  authors?: string[] | null;
  openalex_id?: string | null;
  journal?: string | null;
  senior_author_country?: string | null;
  publication_date?: string | null;
  publication_year?: number | null;
  citing_papers_count: number;
}

export interface DatasetDetailCitation {
  primary_paper_doi: string;
  primary_paper_title?: string | null;
  citing_paper_doi: string;
  citing_paper_title?: string | null;
  citing_authors?: string[] | null;
  citing_journal?: string | null;
  citing_senior_author_country?: string | null;
  citing_publication_date?: string | null;
  citing_publication_year?: number | null;
  classification_status?: string | null;
  classification?: string | null;
  confidence?: number | null;
  reasoning?: string | null;
}

export interface DatasetContributor {
  name: string;
  roles: string[];
}

export interface DatasetDetailResponse {
  dataset: {
    source: string;
    dataset_id: string;
    title: string;
    description?: string | null;
    full_description?: string | null;
    authors?: string[] | null;
    contributors?: DatasetContributor[] | null;
    license?: string | null;
    num_subjects?: number | null;
    modality?: string | null;
    papers?: number | null;
    url?: string | null;
    created_at?: string | null;
    updated_at?: string | null;
  };
  primary_papers: DatasetDetailPaper[];
  citations: DatasetDetailCitation[];
}

/**
 * Fetch datasets from the backend API with optional filters.
 */
export async function fetchDatasets(params?: {
  source?: string;
  modalities?: string[];
  search?: string;
  sort_by?: 'published' | 'papers' | 'title' | 'id' | 'source' | 'modality';
  sort_order?: 'asc' | 'desc';
  limit?: number;
  offset?: number;
}): Promise<DatasetsResponse> {
  try {
    const queryParams = new URLSearchParams();

    if (params?.source) {
      queryParams.append('source', params.source);
    }
    if (params?.modalities?.length) {
      queryParams.append('modality', params.modalities.join(','));
    }
    if (params?.search) {
      queryParams.append('search', params.search);
    }
    if (params?.sort_by) {
      queryParams.append('sort_by', params.sort_by);
    }
    if (params?.sort_order) {
      queryParams.append('sort_order', params.sort_order);
    }
    if (typeof params?.limit === 'number') {
      queryParams.append('limit', String(params.limit));
    }
    if (typeof params?.offset === 'number') {
      queryParams.append('offset', String(params.offset));
    }

    const url = `${API_BASE_URL}/api/datasets${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;

    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`Failed to fetch datasets: ${response.statusText}`);
    }

    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while fetching datasets: ${error?.message || error}`);
  }
}

/**
 * Fetch dataset statistics from the backend API.
 */
export async function fetchDatasetStats(params?: { source?: string; modalities?: string[]; search?: string }): Promise<DatasetStats> {
  try {
    const queryParams = new URLSearchParams();
    if (params?.source) queryParams.append('source', params.source);
    if (params?.modalities?.length) queryParams.append('modality', params.modalities.join(','));
    if (params?.search) queryParams.append('search', params.search);

    const url = `${API_BASE_URL}/api/datasets/stats${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;

    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`Failed to fetch dataset stats: ${response.statusText}`);
    }

    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while fetching dataset stats: ${error?.message || error}`);
  }
}

/**
 * Check API health and database connectivity.
 */
export async function checkApiHealth(): Promise<ApiHealthResponse> {
  try {
    const url = `${API_BASE_URL}/api/health`;

    const response = await fetch(url);

    if (!response.ok) {
      throw new Error(`API health check failed: ${response.statusText}`);
    }

    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while checking API health: ${error?.message || error}`);
  }
}

export async function fetchPaperMappingSummary(params?: {
  source?: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';
}): Promise<PaperMappingSummary> {
  try {
    const queryParams = new URLSearchParams();
    if (params?.source) queryParams.append('source', params.source);
    const url = `${API_BASE_URL}/api/paper-mapping/summary${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch paper mapping summary: ${response.statusText}`);
    }
    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while fetching paper mapping summary: ${error?.message || error}`);
  }
}

export async function fetchPaperMappingDatasets(params?: {
  source?: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';
  search?: string;
  /** Same bucket labels as summary by_classification (e.g. SECONDARY, NEITHER, placeholder). */
  classification_bucket?: string;
  sort_by?:
    | 'mapped_papers'
    | 'citation_edges'
    | 'contexts_extracted'
    | 'latest_primary_publication_date'
    | 'latest_citing_publication_date'
    | 'title'
    | 'id'
    | 'source';
  sort_order?: 'asc' | 'desc';
  limit?: number;
  offset?: number;
}): Promise<PaperMappingDatasetsResponse> {
  try {
    const queryParams = new URLSearchParams();
    if (params?.source) queryParams.append('source', params.source);
    if (params?.search) queryParams.append('search', params.search);
    if (params?.classification_bucket) {
      queryParams.append('classification_bucket', params.classification_bucket);
    }
    if (params?.sort_by) queryParams.append('sort_by', params.sort_by);
    if (params?.sort_order) queryParams.append('sort_order', params.sort_order);
    if (typeof params?.limit === 'number') queryParams.append('limit', String(params.limit));
    if (typeof params?.offset === 'number') queryParams.append('offset', String(params.offset));
    const url = `${API_BASE_URL}/api/paper-mapping/datasets${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch paper mapping datasets: ${response.statusText}`);
    }
    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while fetching paper mapping datasets: ${error?.message || error}`);
  }
}

export async function fetchPaperMappingDatasetDetail(
  source: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC',
  datasetId: string
): Promise<PaperMappingDatasetDetail> {
  try {
    const url = `${API_BASE_URL}/api/paper-mapping/datasets/${encodeURIComponent(source)}/${encodeURIComponent(datasetId)}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch paper mapping dataset detail: ${response.statusText}`);
    }
    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while fetching paper mapping dataset detail: ${error?.message || error}`);
  }
}

export async function fetchDatasetDetail(datasetId: string): Promise<DatasetDetailResponse> {
  try {
    const url = `${API_BASE_URL}/api/datasets/${encodeURIComponent(datasetId)}`;
    const response = await fetch(url);
    if (!response.ok) {
      if (response.status === 404) throw new Error('Dataset not found');
      throw new Error(`Failed to fetch dataset detail: ${response.statusText}`);
    }
    return response.json();
  } catch (error: any) {
    throw new Error(error?.message || 'Network error while fetching dataset detail');
  }
}

export async function fetchPaperMappingCitations(params?: {
  source?: 'CRCNS' | 'DANDI' | 'OpenNeuro' | 'SPARC';
  dataset_id?: string;
  limit?: number;
  offset?: number;
}): Promise<PaperMappingCitationsResponse> {
  try {
    const queryParams = new URLSearchParams();
    if (params?.source) queryParams.append('source', params.source);
    if (params?.dataset_id) queryParams.append('dataset_id', params.dataset_id);
    if (typeof params?.limit === 'number') queryParams.append('limit', String(params.limit));
    if (typeof params?.offset === 'number') queryParams.append('offset', String(params.offset));
    const url = `${API_BASE_URL}/api/paper-mapping/citations${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`Failed to fetch paper mapping citations: ${response.statusText}`);
    }
    return response.json();
  } catch (error: any) {
    throw new Error(`Network error while fetching paper mapping citations: ${error?.message || error}`);
  }
}
