/** Loading indicators shared by the home and Paper Mapping tabs. */

/** The small spinning circle shown next to "Loading…" text. */
export function Spinner() {
  return (
    <span
      aria-hidden="true"
      className="inline-block h-4 w-4 shrink-0 animate-spin rounded-full border-2 border-line-rule border-t-accent motion-reduce:animate-none"
    />
  );
}

/** A pulsing placeholder block; `className` sets its size and rounding. */
export function Skeleton({ className }: { className: string }) {
  return <div aria-hidden="true" className={`animate-skeleton bg-line-2 motion-reduce:animate-none ${className}`} />;
}
