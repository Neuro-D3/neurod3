/**
 * API service for fetching neuroscience datasets from the backend.
 */

const API_BASE_URL = process.env.REACT_APP_API_URL || 'http://localhost:8000';

export interface Dataset {
  source: 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet';
  id: string;
  title: string;
  modality: string;
  citations: number;
  url: string;
  description?: string;
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

/**
 * Fetch datasets from the backend API with optional filters.
 */
export async function fetchDatasets(params?: {
  source?: string;
  modality?: string;
  search?: string;
}): Promise<DatasetsResponse> {
  const queryParams = new URLSearchParams();

  if (params?.source) {
    queryParams.append('source', params.source);
  }
  if (params?.modality) {
    queryParams.append('modality', params.modality);
  }
  if (params?.search) {
    queryParams.append('search', params.search);
  }

  const url = `${API_BASE_URL}/api/datasets${queryParams.toString() ? `?${queryParams.toString()}` : ''}`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch datasets: ${response.statusText}`);
  }

  return response.json();
}

/**
 * Fetch dataset statistics from the backend API.
 */
export async function fetchDatasetStats(): Promise<DatasetStats> {
  const url = `${API_BASE_URL}/api/datasets/stats`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`Failed to fetch dataset stats: ${response.statusText}`);
  }

  return response.json();
}

/**
 * Check API health and database connectivity.
 */
export async function checkApiHealth(): Promise<ApiHealthResponse> {
  const url = `${API_BASE_URL}/api/health`;

  const response = await fetch(url);

  if (!response.ok) {
    throw new Error(`API health check failed: ${response.statusText}`);
  }

  return response.json();
}
