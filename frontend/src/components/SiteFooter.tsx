import React from 'react';

import berkeleyLabLogo from '../assets/partners/berkeley-lab.svg';
import catalystNeuroLogo from '../assets/partners/catalystneuro.png';
import duraLabsMark from '../assets/partners/duralabs.svg';
import foresightLogo from '../assets/partners/foresight.svg';

// Git SHA baked into the image at build time (CI --build-arg GIT_SHA -> Dockerfile
// ENV REACT_APP_GIT_SHA, inlined by CRA). Defaults to "dev" locally.
const GIT_SHA = process.env.REACT_APP_GIT_SHA || 'dev';
const REPO_URL = 'https://github.com/Neuro-D3/neurod3';

interface Partner {
  name: string;
  href: string;
  logo: string;
  /** Height class; the logos fill their boxes differently, so each is sized to look even. */
  height: string;
  /** The logo is a mark without the name, so the name is written next to it. */
  showName?: boolean;
}

// Logos from each organization's own website.
const PARTNERS: Partner[] = [
  { name: 'Foresight Institute', href: 'https://foresight.org/', logo: foresightLogo, height: 'h-5' },
  { name: 'CatalystNeuro', href: 'https://catalystneuro.com/', logo: catalystNeuroLogo, height: 'h-4' },
  { name: 'DuraLabs', href: 'https://duralabs.ai', logo: duraLabsMark, height: 'h-3.5', showName: true },
  { name: 'Berkeley Lab', href: 'https://linktr.ee/BerkeleyLab', logo: berkeleyLabLogo, height: 'h-4' },
];

const ARCHIVES: Array<{ name: string; href: string }> = [
  { name: 'DANDI Archive', href: 'https://dandiarchive.org' },
  { name: 'OpenNeuro', href: 'https://openneuro.org' },
  { name: 'CRCNS', href: 'https://crcns.org' },
  { name: 'SPARC', href: 'https://sparc.science' },
];

/**
 * Footer on every page: where the data comes from, the organizations that
 * developed the project, and the build. Logos are muted until hovered or
 * focused, so four brand colours don't compete with the data above.
 */
export function SiteFooter() {
  return (
    <footer className="border-t border-line bg-surface">
      <div className="mx-auto flex max-w-[1440px] flex-col gap-4 px-4 py-5 text-xs leading-normal text-muted sm:px-6 lg:px-8 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex flex-col gap-1.5">
          <p className="m-0">
            Dataset metadata from{' '}
            {ARCHIVES.map((a, i) => (
              <React.Fragment key={a.name}>
                {i > 0 && (i === ARCHIVES.length - 1 ? ' and ' : ', ')}
                <a href={a.href} target="_blank" rel="noopener noreferrer">
                  {a.name}
                </a>
              </React.Fragment>
            ))}
            . Citations from OpenAlex. Reuse labels are assigned from each citing paper's full text.
          </p>
          <p className="m-0">
            Code:{' '}
            <a href={REPO_URL} target="_blank" rel="noopener noreferrer">
              github.com/Neuro-D3/neurod3
            </a>
            {' · '}
            Build{' '}
            {GIT_SHA === 'dev' ? (
              <span className="font-mono">dev</span>
            ) : (
              <a href={`${REPO_URL}/commit/${GIT_SHA}`} target="_blank" rel="noopener noreferrer" className="font-mono">
                {GIT_SHA.slice(0, 7)}
              </a>
            )}
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-x-5 gap-y-3">
          <span className="font-bold uppercase tracking-[0.06em]">Developed by</span>
          <ul className="m-0 flex list-none flex-wrap items-center gap-x-5 gap-y-3 p-0">
            {PARTNERS.map((partner) => (
              <li key={partner.name}>
                <a
                  href={partner.href}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-center gap-1 rounded opacity-70 grayscale transition hover:opacity-100 hover:grayscale-0 focus-visible:opacity-100 focus-visible:grayscale-0"
                >
                  <img src={partner.logo} alt={partner.showName ? '' : partner.name} className={`${partner.height} w-auto dark:brightness-0 dark:invert`} />
                  {partner.showName && (
                    <span className="text-[9px] font-semibold leading-none tracking-tight text-ink">{partner.name}</span>
                  )}
                </a>
              </li>
            ))}
          </ul>
        </div>
      </div>
    </footer>
  );
}
