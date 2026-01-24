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
  source: 'DANDI' | 'Kaggle' | 'OpenNeuro' | 'PhysioNet';
  id: string;
  title: string;
  modality: string | null;
  tags?: string | null;
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
  modalities?: string[];
  search?: string;
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
export async function fetchDatasetStats(params?: { source?: string; modalities?: string[] }): Promise<DatasetStats> {
  try {
    const queryParams = new URLSearchParams();
    if (params?.source) queryParams.append('source', params.source);
    if (params?.modalities?.length) queryParams.append('modality', params.modalities.join(','));

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
