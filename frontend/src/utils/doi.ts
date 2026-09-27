/** Resolver link for a DOI, keeping its slashes readable. */
export function doiUrl(doi: string) {
  return `https://doi.org/${encodeURIComponent(doi).replace(/%2F/gi, '/')}`;
}
