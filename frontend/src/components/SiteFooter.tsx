import React from 'react';

import berkeleyLabLogo from '../assets/partners/berkeley-lab.svg';
import catalystNeuroLogo from '../assets/partners/catalystneuro.png';
import duraLabsMark from '../assets/partners/duralabs.svg';
import foresightLogo from '../assets/partners/foresight.svg';

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

/**
 * Footer on every page: the organizations that developed the project.
 * Logos are muted until hovered or focused, so four brand colours don't
 * compete with the data above.
 */
export function SiteFooter() {
  return (
    <footer className="border-t border-slate-200 bg-white">
      <div className="mx-auto max-w-7xl px-4 py-6 sm:px-6 lg:px-8">
        <p className="text-center text-xs font-semibold uppercase tracking-wider text-slate-500">Developed by</p>
        <ul className="mt-3 flex flex-wrap items-center justify-center gap-x-6 gap-y-3">
          {PARTNERS.map((partner) => (
            <li key={partner.name}>
              <a
                href={partner.href}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1 rounded-md opacity-70 grayscale transition hover:opacity-100 hover:grayscale-0 focus-visible:opacity-100 focus-visible:grayscale-0 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-blue-500 focus-visible:ring-offset-2"
              >
                <img src={partner.logo} alt={partner.showName ? '' : partner.name} className={`${partner.height} w-auto`} />
                {partner.showName && (
                  <span className="text-[9px] font-semibold leading-none tracking-tight text-slate-800">{partner.name}</span>
                )}
              </a>
            </li>
          ))}
        </ul>
      </div>
    </footer>
  );
}
