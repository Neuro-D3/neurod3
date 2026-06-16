import React from 'react';

// Git SHA baked into the image at build time (CI passes --build-arg GIT_SHA ->
// Dockerfile ENV REACT_APP_GIT_SHA, inlined by CRA). Defaults to "dev" locally.
const SHA = process.env.REACT_APP_GIT_SHA || 'dev';
const REPO = 'https://github.com/Neuro-D3/neurod3';

const PARTNERS: { name: string; href?: string }[] = [
  { name: 'Foresight Institute' },
  { name: 'Catalyst Neuro', href: 'https://catalystneuro.com/' },
  { name: 'Dura Labs', href: 'https://duralabs.ai' },
  { name: 'Berkeley Lab', href: 'https://linktr.ee/BerkeleyLab' },
];

export function BuildFooter() {
  const isDev = !SHA || SHA === 'dev';
  return (
    <footer className="mx-auto max-w-7xl px-4 py-6 text-center text-xs text-slate-400 sm:px-6 lg:px-8">
      <p>
        Developed by{' '}
        {PARTNERS.map((p, i) => (
          <React.Fragment key={p.name}>
            {i > 0 && <span className="mx-1.5 text-slate-300">|</span>}
            {p.href ? (
              <a
                href={p.href}
                target="_blank"
                rel="noreferrer"
                className="hover:text-slate-600"
              >
                {p.name}
              </a>
            ) : (
              <span>{p.name}</span>
            )}
          </React.Fragment>
        ))}
      </p>
      <p className="mt-1">
        build{' '}
        {isDev ? (
          'dev'
        ) : (
          <a
            href={`${REPO}/commit/${SHA}`}
            target="_blank"
            rel="noreferrer"
            className="underline hover:text-slate-600"
          >
            {SHA.slice(0, 7)}
          </a>
        )}
      </p>
    </footer>
  );
}

export default BuildFooter;
