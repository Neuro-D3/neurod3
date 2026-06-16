import React from 'react';

// Git SHA baked into the image at build time (CI passes --build-arg GIT_SHA ->
// Dockerfile ENV REACT_APP_GIT_SHA, inlined by CRA). Defaults to "dev" locally.
const SHA = process.env.REACT_APP_GIT_SHA || 'dev';
const REPO = 'https://github.com/Neuro-D3/neurod3';

export function BuildFooter() {
  const isDev = !SHA || SHA === 'dev';
  return (
    <footer className="mx-auto max-w-7xl px-4 py-3 text-xs text-slate-400 sm:px-6 lg:px-8">
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
    </footer>
  );
}

export default BuildFooter;
