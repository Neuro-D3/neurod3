/**
 * Dataset licenses as short SPDX-style labels with a link to the license
 * text. Archives store them in many shapes: DANDI as "spdx:CC-BY-4.0"
 * (sometimes two, comma-separated), OpenNeuro as "CC0" or a sentence, SPARC
 * as "Creative Commons Attribution". A license we can't recognise keeps its
 * own words, shortened, with no link; an empty or placeholder value ("-")
 * gives nothing.
 */
export interface LicenseLink {
  /** Short label: "CC-BY-4.0", "CC0-1.0", "PDDL-1.0", or the archive's text shortened. */
  label: string;
  /** The license text, or null when we don't know where it lives. */
  url: string | null;
  /** The archive's own wording, for a tooltip. */
  title: string;
}

const MAX_LABEL = 40;

function shorten(text: string): string {
  return text.length > MAX_LABEL ? `${text.slice(0, MAX_LABEL - 1).trimEnd()}…` : text;
}

/** First http(s) URL in free text, without trailing punctuation. */
function urlIn(text: string): string | null {
  const m = /https?:\/\/[^\s)'"]+/i.exec(text);
  return m ? m[0].replace(/[.,;]+$/, '') : null;
}

function creativeCommons(text: string): Omit<LicenseLink, 'title'> | null {
  const t = text.toLowerCase();
  if (/\bcc[0o]\b|cc0-1\.0|creative commons cc0/.test(t)) {
    return { label: 'CC0-1.0', url: 'https://creativecommons.org/publicdomain/zero/1.0/' };
  }
  const isCc = /creative\s*commons|\bcc[\s-]*by\b|\bccby\b|^cc$|creativecommons\.org\/licenses/.test(t);
  const attribution = /\bby\b|attribution|ccby/.test(t);
  if (!isCc && !/^attribution\b/.test(t)) return null;
  if (!attribution) return { label: 'Creative Commons', url: 'https://creativecommons.org/licenses/' };
  const nc = /\bnc\b|non-?commercial/.test(t);
  const nd = /\bnd\b|no-?deriv/.test(t);
  const sa = /\bsa\b|share-?alike/.test(t);
  const slug = ['by', nc && 'nc', nd ? 'nd' : sa && 'sa'].filter(Boolean).join('-');
  const version = /\b([1-4]\.0)\b/.exec(t)?.[1] ?? null;
  // No version given: the label says so, and the link goes to the current (4.0) text.
  return {
    label: `CC-${slug.toUpperCase()}${version ? `-${version}` : ''}`,
    url: `https://creativecommons.org/licenses/${slug}/${version ?? '4.0'}/`,
  };
}

/** "TODO: choose a license", "NA - not for public distribution", "Not Specified": no license yet. */
const PLACEHOLDER = /^(todo\b|n\/?a\b|not specified|none\b)/i;

function knownLicense(text: string): Omit<LicenseLink, 'title'> | null {
  const t = text.toLowerCase();
  if (PLACEHOLDER.test(t)) return null;
  if (/pddl|ppdl|public domain dedication and licen[cs]e/.test(t)) {
    return { label: 'PDDL-1.0', url: 'https://opendatacommons.org/licenses/pddl/1-0/' };
  }
  if (/open data commons attribution|odc-by/.test(t)) {
    return { label: 'ODC-By-1.0', url: 'https://opendatacommons.org/licenses/by/1-0/' };
  }
  if (/open data commons open database|\bodbl\b/.test(t)) {
    return { label: 'ODbL-1.0', url: 'https://opendatacommons.org/licenses/odbl/1-0/' };
  }
  const cc = creativeCommons(text);
  if (cc) return cc;
  if (/^mit\b/.test(t)) return { label: 'MIT', url: 'https://opensource.org/license/mit' };
  if (/apache/.test(t)) return { label: 'Apache-2.0', url: 'https://www.apache.org/licenses/LICENSE-2.0' };
  if (/lesser general public|\blgpl/.test(t)) {
    return /3\.0/.test(t)
      ? { label: 'LGPL-3.0', url: 'https://www.gnu.org/licenses/lgpl-3.0.html' }
      : { label: 'LGPL', url: 'https://www.gnu.org/licenses/lgpl.html' };
  }
  if (/general public licen[cs]e|\bgpl/.test(t)) {
    return /3\.0/.test(t)
      ? { label: 'GPL-3.0', url: 'https://www.gnu.org/licenses/gpl-3.0.html' }
      : { label: 'GPL', url: 'https://www.gnu.org/licenses/gpl.html' };
  }
  if (/community data license agreement/.test(t)) {
    return { label: /sharing/.test(t) ? 'CDLA-Sharing' : 'CDLA-Permissive', url: 'https://cdla.dev/' };
  }
  return null;
}

export function licenseLinks(raw: string | null | undefined): LicenseLink[] {
  const text = (raw ?? '').replace(/\s+/g, ' ').trim();
  if (!text || /^[-~.\s]*$/.test(text)) return [];
  // DANDI lists several SPDX ids separated by commas.
  const parts = /^spdx:/i.test(text) ? text.split(',').map((p) => p.trim()).filter(Boolean) : [text];
  return parts.map((part) => {
    const bare = part.replace(/^spdx:/i, '');
    const known = knownLicense(bare);
    if (known) return { ...known, title: part };
    return { label: shorten(bare), url: PLACEHOLDER.test(bare) ? null : urlIn(bare), title: part };
  });
}
