import React, { useEffect, useRef, useState } from 'react';
import { Link } from 'react-router-dom';

/**
 * The small set of building blocks every page is made of, in the poster's
 * design language: white cards with hairline borders and a heavy rule under
 * the title, numbered section badges, lettered subsections, small-caps
 * eyebrows, square-cornered metadata chips, and two button weights.
 */

export function cx(...parts: Array<string | false | null | undefined>): string {
  return parts.filter(Boolean).join(' ');
}

/* ───────────────────────── Cards and headings ───────────────────────── */

export const CARD_CLASS = 'rounded-md border border-line bg-surface shadow-card';

export function Card({
  className,
  children,
  as: Tag = 'section',
  ...rest
}: React.HTMLAttributes<HTMLElement> & { as?: 'section' | 'div' | 'article' | 'aside' }) {
  return (
    <Tag className={cx(CARD_CLASS, 'flex flex-col gap-3.5 px-[22px] pb-[22px] pt-5', className)} {...rest}>
      {children}
    </Tag>
  );
}

/**
 * A card's title row: an optional number badge, the title, and anything
 * pushed to the right (a link, a count). The heavy rule beneath is the
 * poster's.
 */
export function CardTitle({
  number,
  id,
  children,
  aside,
  className,
}: {
  number?: number | string;
  id?: string;
  children: React.ReactNode;
  aside?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cx('flex items-center gap-3 border-b-[3px] border-line-rule pb-2.5', className)}>
      {number !== undefined && (
        <span
          aria-hidden="true"
          className="flex h-[30px] w-[30px] shrink-0 items-center justify-center rounded-[5px] bg-accent text-base font-bold text-accent-fg"
        >
          {number}
        </span>
      )}
      <h2 id={id} className="m-0 text-[22px] font-bold leading-tight text-ink">
        {children}
      </h2>
      {aside && <div className="ml-auto flex items-center gap-3 text-[13.5px]">{aside}</div>}
    </div>
  );
}

/** "A  Linked Papers": a lettered subsection heading inside a card. */
export function SubTitle({
  letter,
  children,
  aside,
  className,
}: {
  letter?: string;
  children: React.ReactNode;
  aside?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cx('flex items-center justify-between gap-3', className)}>
      <h3 className="m-0 flex gap-2.5 text-[15.5px] font-bold text-ink">
        {letter && <span className="text-link">{letter}</span>}
        {children}
      </h3>
      {aside}
    </div>
  );
}

/** Small-caps label above a value or a group of chips. */
export function Eyebrow({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <span className={cx('text-xs font-bold uppercase tracking-[0.06em] text-muted', className)}>{children}</span>
  );
}

/** The grey note under a chart or table that says where the numbers come from. */
export function Note({ children, className }: { children: React.ReactNode; className?: string }) {
  return <p className={cx('m-0 text-[13.5px] leading-normal text-ink-3', className)}>{children}</p>;
}

/* ───────────────────────── Chips and tags ───────────────────────── */

/** Pale-blue metadata chip (modality, region, library). `mono` for identifiers. */
export function Chip({
  children,
  mono = false,
  active = false,
  title,
  onClick,
  className,
}: {
  children: React.ReactNode;
  mono?: boolean;
  active?: boolean;
  title?: string;
  onClick?: () => void;
  className?: string;
}) {
  const base = cx(
    'inline-flex items-center rounded border px-[9px] py-1 leading-none',
    mono ? 'font-mono text-xs' : 'text-[13px]',
    active ? 'border-accent bg-accent text-accent-fg' : 'border-chip-border bg-chip-bg text-chip-text',
    onClick && !active && 'hover:border-reuse-soft hover:bg-reuse-tint',
    className,
  );
  if (onClick) {
    return (
      <button type="button" onClick={onClick} title={title} aria-pressed={active} className={base}>
        {children}
      </button>
    );
  }
  return (
    <span title={title} className={base}>
      {children}
    </span>
  );
}

/**
 * Outlined tag on the navy masthead (archive, license). `warn` is the amber
 * junk flag; `href` makes it a link that opens in a new tab.
 */
export function MastTag({
  children,
  title,
  tone = 'plain',
  href,
}: {
  children: React.ReactNode;
  title?: string;
  tone?: 'plain' | 'warn';
  href?: string;
}) {
  const classes = cx(
    'inline-flex max-w-[22rem] items-center gap-1 rounded border px-2 py-[3px] text-[12.5px] font-bold leading-none',
    tone === 'warn' ? 'border-[#F2C14E] bg-[#F2C14E] text-navy' : 'border-navy-tag text-white',
  );
  if (href) {
    return (
      <a href={href} target="_blank" rel="noopener noreferrer" title={title} className={cx(classes, 'hover:border-white hover:text-white')}>
        <span className="truncate">{children}</span>
        <ExternalIcon className="shrink-0" />
      </a>
    );
  }
  return (
    <span title={title} className={classes}>
      <span className="truncate">{children}</span>
    </span>
  );
}

export type TagTone = 'reuse' | 'mention' | 'primary' | 'neutral' | 'tan' | 'warn' | 'danger';

const TAG_TONES: Record<TagTone, string> = {
  reuse: 'bg-reuse-tint text-reuse-ink',
  mention: 'bg-mention-tint text-mention-ink',
  primary: 'bg-primary-tint text-primary-ink',
  neutral: 'bg-neither-tint text-neither-ink',
  tan: 'bg-tan-tint text-tan-ink',
  warn: 'bg-warn-tint text-warn-ink',
  danger: 'bg-danger-tint text-danger-ink',
};

/** Solid-tint role tag used in tables (Finding / Method / Review in the poster). */
export function Tag({
  tone = 'neutral',
  children,
  title,
  className,
}: {
  tone?: TagTone;
  children: React.ReactNode;
  title?: string;
  className?: string;
}) {
  return (
    <span
      title={title}
      className={cx('inline-flex h-[22px] items-center rounded px-[7px] text-xs font-bold leading-none', TAG_TONES[tone], className)}
    >
      {children}
    </span>
  );
}

/* ───────────────────────── Buttons ───────────────────────── */

type ButtonVariant = 'primary' | 'secondary' | 'ghost';

const BUTTON_VARIANTS: Record<ButtonVariant, string> = {
  primary: 'border border-accent bg-accent text-accent-fg hover:border-accent-hover hover:bg-accent-hover hover:text-accent-fg',
  secondary: 'border border-accent bg-surface text-ink hover:bg-panel hover:text-ink',
  ghost: 'border border-line bg-surface text-ink-2 hover:bg-panel hover:text-ink-2',
};

const BUTTON_BASE =
  'inline-flex h-[38px] items-center justify-center gap-2 whitespace-nowrap rounded-[5px] px-4 text-sm font-bold leading-none transition-colors disabled:cursor-not-allowed disabled:opacity-50';

export function Button({
  variant = 'secondary',
  size,
  className,
  children,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { variant?: ButtonVariant; size?: 'sm' }) {
  return (
    <button
      type="button"
      className={cx(BUTTON_BASE, BUTTON_VARIANTS[variant], size === 'sm' && 'h-8 px-3 text-[13px]', className)}
      {...rest}
    >
      {children}
    </button>
  );
}

export function ButtonLink({
  variant = 'primary',
  size,
  className,
  children,
  to,
  ...rest
}: React.AnchorHTMLAttributes<HTMLAnchorElement> & { variant?: ButtonVariant; size?: 'sm'; to?: string }) {
  const classes = cx(BUTTON_BASE, BUTTON_VARIANTS[variant], size === 'sm' && 'h-8 px-3 text-[13px]', 'no-underline hover:no-underline', className);
  if (to) {
    return (
      <Link to={to} className={classes}>
        {children}
      </Link>
    );
  }
  return (
    <a className={classes} {...rest}>
      {children}
    </a>
  );
}

/* ───────────────────────── Tabs ───────────────────────── */

/**
 * The underlined tab strip under a masthead. A tab is a route (`to`), an
 * anchor (`href`) or a button (`onClick`); the active one carries a navy
 * underline.
 */
export function Tabs({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <nav aria-label={label} className="flex gap-1 overflow-x-auto">
      {children}
    </nav>
  );
}

export function Tab({
  active = false,
  to,
  href,
  onClick,
  count,
  children,
}: {
  active?: boolean;
  to?: string;
  href?: string;
  onClick?: () => void;
  count?: number | string;
  children: React.ReactNode;
}) {
  const classes = cx(
    'flex h-10 shrink-0 items-center gap-[7px] border-b-[3px] px-3.5 text-[14.5px] no-underline hover:no-underline',
    active ? 'border-accent font-bold text-ink hover:text-ink' : 'border-transparent text-muted hover:text-ink',
  );
  const badge =
    count !== undefined ? (
      <span className="rounded-full bg-line-3 px-[7px] py-px text-xs tabular-nums text-ink-3">{count}</span>
    ) : null;
  if (to) {
    return (
      <Link to={to} aria-current={active ? 'page' : undefined} className={classes}>
        {children}
        {badge}
      </Link>
    );
  }
  if (href) {
    return (
      <a href={href} aria-current={active ? 'page' : undefined} className={classes}>
        {children}
        {badge}
      </a>
    );
  }
  return (
    <button type="button" onClick={onClick} aria-pressed={active} className={classes}>
      {children}
      {badge}
    </button>
  );
}

/* ───────────────────────── Stats and tables ───────────────────────── */

/** A row of stat tiles separated by hairlines, as in the poster's Reuse Metrics. */
export function StatGrid({ children, columns, className }: { children: React.ReactNode; columns: number; className?: string }) {
  return (
    <dl
      className={cx('m-0 grid rounded-[5px] border border-line-2', className)}
      style={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}
    >
      {children}
    </dl>
  );
}

export function StatTile({
  label,
  value,
  valueClassName,
  children,
  first = false,
  loading = false,
}: {
  label: string;
  value?: React.ReactNode;
  valueClassName?: string;
  children?: React.ReactNode;
  first?: boolean;
  loading?: boolean;
}) {
  return (
    <div className={cx('flex min-w-0 flex-col gap-[3px] p-3', !first && 'border-l border-line-2')} aria-busy={loading || undefined}>
      <dt className="text-[11px] font-bold uppercase tracking-[0.06em] text-muted">{label}</dt>
      <dd className="m-0 flex flex-col gap-0.5">
        {loading ? (
          <span className="mt-1 h-8 w-14 animate-skeleton rounded bg-line-2 motion-reduce:animate-none" aria-hidden="true" />
        ) : (
          <span className={cx('text-[32px] font-bold leading-[1.05] tabular-nums text-ink', valueClassName)}>{value}</span>
        )}
        {children}
      </dd>
    </div>
  );
}

/** Two-column facts table: label left, bold value right. */
export function FactTable({
  caption,
  rows,
}: {
  caption?: React.ReactNode;
  rows: Array<{ label: React.ReactNode; value: React.ReactNode; key?: string }>;
}) {
  return (
    <table className="w-full border-collapse text-[14.5px]">
      {caption && (
        <thead>
          <tr>
            <th scope="col" className="pb-1.5 text-left font-normal" />
            <th scope="col" className="pb-1.5 text-right text-sm font-bold text-link">
              {caption}
            </th>
          </tr>
        </thead>
      )}
      <tbody>
        {rows.map((row, i) => (
          <tr key={row.key ?? i} className="border-t border-line-inner">
            <td className="whitespace-nowrap py-[7px] pr-3 align-top text-ink-2">{row.label}</td>
            <td className="break-words py-[7px] text-right font-bold text-ink">{row.value}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

/* ───────────────────────── Inputs ───────────────────────── */

export const INPUT_CLASS =
  'h-[38px] rounded-[5px] border border-line bg-surface px-3 text-sm text-ink placeholder:text-muted focus:border-focus focus:outline-none';

/** A read-only command with a copy button; says "Copied" for a moment after. */
export function CopyField({ value, label }: { value: string; label: string }) {
  const [copied, setCopied] = useState(false);
  const timer = useRef<number | null>(null);
  useEffect(() => () => {
    if (timer.current !== null) window.clearTimeout(timer.current);
  }, []);
  const copy = async () => {
    try {
      await navigator.clipboard.writeText(value);
      setCopied(true);
      if (timer.current !== null) window.clearTimeout(timer.current);
      timer.current = window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  };
  return (
    <div className="flex h-10 items-center justify-between gap-2 rounded-[5px] border border-line-2 bg-panel pl-3 pr-1">
      <code className="min-w-0 truncate font-mono text-[13px] text-ink">{value}</code>
      <button
        type="button"
        onClick={copy}
        aria-label={copied ? 'Copied' : label}
        title={copied ? 'Copied' : label}
        className="flex h-8 shrink-0 items-center justify-center gap-1 rounded px-2 text-muted hover:bg-line-3 hover:text-ink"
      >
        {copied ? (
          <span className="text-xs font-bold text-link">Copied</span>
        ) : (
          <CopyIcon />
        )}
      </button>
    </div>
  );
}

export function CopyIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <rect x="9" y="9" width="12" height="12" rx="2" />
      <path d="M5 15V5a2 2 0 0 1 2-2h10" />
    </svg>
  );
}

/** The ↗ that marks a link leaving the site. */
export function ExternalIcon({ className }: { className?: string }) {
  return (
    <svg className={cx('inline-block', className)} width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true">
      <path d="M7 17 17 7" />
      <path d="M8 7h9v9" />
    </svg>
  );
}
