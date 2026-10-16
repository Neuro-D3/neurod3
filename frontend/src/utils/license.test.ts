import { licenseLinks } from './license';

const one = (raw: string) => {
  const [l] = licenseLinks(raw);
  return { label: l.label, url: l.url };
};

describe('licenseLinks', () => {
  it('reads DANDI SPDX ids, including two in one value', () => {
    expect(one('spdx:CC-BY-4.0')).toEqual({ label: 'CC-BY-4.0', url: 'https://creativecommons.org/licenses/by/4.0/' });
    expect(one('spdx:CC-BY-NC-4.0')).toEqual({ label: 'CC-BY-NC-4.0', url: 'https://creativecommons.org/licenses/by-nc/4.0/' });
    expect(licenseLinks('spdx:CC0-1.0, spdx:CC-BY-4.0').map((l) => l.label)).toEqual(['CC0-1.0', 'CC-BY-4.0']);
  });

  it('reads OpenNeuro short names and typos', () => {
    for (const raw of ['CC0', 'CCO', 'CCo', 'Creative Commons CC0 1.0']) {
      expect(one(raw)).toEqual({ label: 'CC0-1.0', url: 'https://creativecommons.org/publicdomain/zero/1.0/' });
    }
    for (const raw of ['PDDL', 'PPDL', 'PDDL v1.0', 'PDDL (http://opendatacommons.org/licenses/pddl/)']) {
      expect(one(raw).label).toBe('PDDL-1.0');
    }
    expect(one('CC-BY-SA 4.0')).toEqual({ label: 'CC-BY-SA-4.0', url: 'https://creativecommons.org/licenses/by-sa/4.0/' });
  });

  it('reads license sentences', () => {
    expect(one('This dataset is made available under the Public Domain Dedication and License v1.0, whose full text can be found at http://www.opendatacommons.org/licenses/pddl/1.0/.').label).toBe('PDDL-1.0');
    expect(one('This data is made available under the Creative Commons BY-SA 4.0 International License.').label).toBe('CC-BY-SA-4.0');
    expect(one('Attribution-NonCommercial-NoDerivatives 4.0 International (CC BY-NC-ND 4.0)').label).toBe('CC-BY-NC-ND-4.0');
    expect(one('https://creativecommons.org/licenses/by-nc-sa/4.0/').label).toBe('CC-BY-NC-SA-4.0');
  });

  it('keeps an unversioned Creative Commons label unversioned', () => {
    expect(one('Creative Commons Attribution')).toEqual({ label: 'CC-BY', url: 'https://creativecommons.org/licenses/by/4.0/' });
    expect(one('Creative commons')).toEqual({ label: 'Creative Commons', url: 'https://creativecommons.org/licenses/' });
  });

  it('reads the software and data licenses SPARC uses', () => {
    expect(one('MIT').label).toBe('MIT');
    expect(one('GNU Lesser General Public License')).toEqual({ label: 'LGPL', url: 'https://www.gnu.org/licenses/lgpl.html' });
    expect(one('GNU General Public License v3.0').label).toBe('GPL-3.0');
    expect(one('Open Data Commons Attribution').label).toBe('ODC-By-1.0');
    expect(one('Community Data License Agreement – Permissive').label).toBe('CDLA-Permissive');
  });

  it('leaves unknown text unlinked and shortened, and placeholders out', () => {
    expect(one('Not Specified')).toEqual({ label: 'Not Specified', url: null });
    expect(one('TODO: choose a license, e.g. PDDL (http://opendatacommons.org/licenses/pddl/)').url).toBeNull();
    // A URL in free text is only linked when it is on a license site.
    expect(one('See https://example.org/terms for the terms')).toEqual({
      label: 'See https://example.org/terms for the t…',
      url: null,
    });
    expect(one('NIDO Terms and Conditions, see http://oma.od.nih.gov/public/ms/privacy/pafiles/0156.htm').url).toBeNull();
    expect(one('Released under https://www.gnu.org/licenses/agpl-3.0.html').url).toBe('https://www.gnu.org/licenses/agpl-3.0.html');
    // A look-alike host is not a license site.
    expect(one('Terms: https://notgnu.org/x').url).toBeNull();
    const long = one('NIDO Terms and Conditions In accessing data from one or more of the datasets');
    expect(long.url).toBeNull();
    expect(long.label.length).toBeLessThanOrEqual(40);
    expect(long.label.endsWith('…')).toBe(true);
    expect(licenseLinks('-')).toEqual([]);
    expect(licenseLinks('  ')).toEqual([]);
    expect(licenseLinks(null)).toEqual([]);
  });
});
