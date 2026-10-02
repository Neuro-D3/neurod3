/** Loading indicators shared by the home and Paper Mapping tabs. */

/** The small spinning circle shown next to "Loading…" text. */
export function Spinner({ darkMode = false }: { darkMode?: boolean }) {
  return (
    <span
      aria-hidden="true"
      className={`inline-block h-4 w-4 shrink-0 animate-spin rounded-full border-2 motion-reduce:animate-none ${
        darkMode ? 'border-white/25 border-t-white' : 'border-slate-300 border-t-slate-700'
      }`}
    />
  );
}

/** A pulsing placeholder block; `className` sets its size and rounding. */
export function Skeleton({ className, darkMode = false }: { className: string; darkMode?: boolean }) {
  return (
    <div
      aria-hidden="true"
      className={`animate-skeleton motion-reduce:animate-none ${darkMode ? 'bg-white/20' : 'bg-slate-300'} ${className}`}
    />
  );
}
