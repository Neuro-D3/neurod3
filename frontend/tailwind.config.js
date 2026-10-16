/** @type {import('tailwindcss').Config} */

// NeuroD3 design tokens. The palette is the SfN 2026 dataset-card poster's:
// a navy masthead, a cool grey canvas, white cards with hairline borders, and
// two data hues (blue = reuse, orange = mention). Everything else is neutral.
//
// Every colour except the masthead navy is a CSS variable (src/index.css), so
// the `dark` class on <html> swaps the whole palette at once.
const v = (name) => `rgb(var(--${name}) / <alpha-value>)`;

module.exports = {
  darkMode: 'class',
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Masthead: navy in both themes.
        navy: { DEFAULT: '#021242', 700: '#0B1F5C', 600: '#16306F', muted: '#A9B6D3', sub: '#C9D3E6', tag: '#6F80A8', line: '#1E2F63' },
        // Text
        ink: { DEFAULT: v('ink'), 2: v('ink-2'), 3: v('ink-3') },
        muted: v('muted'),
        // Surfaces: the page, cards and inputs, and the grey wells inside cards.
        canvas: v('canvas'),
        surface: v('surface'),
        panel: { DEFAULT: v('panel'), head: v('panel-head') },
        line: { DEFAULT: v('line'), 2: v('line-2'), 3: v('line-3'), rule: v('line-rule'), inner: v('line-inner') },
        grid: v('grid'),
        // Links and focus
        link: { DEFAULT: v('link'), hover: v('link-hover') },
        focus: v('focus'),
        // Filled controls (primary buttons, active chips, section numbers): navy on light, pale blue on dark.
        accent: { DEFAULT: v('accent'), fg: v('accent-fg'), hover: v('accent-hover') },
        // Data hues
        reuse: { DEFAULT: v('reuse'), soft: v('reuse-soft'), tint: v('reuse-tint'), ink: v('reuse-ink') },
        mention: { DEFAULT: v('mention'), soft: v('mention-soft'), tint: v('mention-tint'), ink: v('mention-ink') },
        primary: { DEFAULT: v('primary'), soft: v('primary-soft'), tint: v('primary-tint'), ink: v('primary-ink') },
        bar: { DEFAULT: v('bar'), soft: v('bar-soft'), hist: v('bar-hist') },
        neither: { tint: v('neither-tint'), ink: v('neither-ink') },
        tan: { tint: v('tan-tint'), ink: v('tan-ink') },
        warn: { DEFAULT: v('warn'), tint: v('warn-tint'), ink: v('warn-ink') },
        danger: { DEFAULT: v('danger'), tint: v('danger-tint'), ink: v('danger-ink') },
        // Metadata chips
        chip: { bg: v('chip-bg'), border: v('chip-border'), text: v('chip-text') },
      },
      fontFamily: {
        sans: ['Arial', '"Helvetica Neue"', 'Helvetica', 'sans-serif'],
        mono: ['"JetBrains Mono"', 'ui-monospace', 'Menlo', 'monospace'],
      },
      boxShadow: {
        card: '0 1px 2px rgba(2, 18, 66, 0.06)',
        pop: '0 8px 24px rgba(2, 18, 66, 0.14)',
      },
      // Side panels slide in from the right edge (same curve as the drill-down).
      keyframes: {
        'panel-in': { from: { transform: 'translateX(100%)' }, to: { transform: 'translateX(0)' } },
        'panel-out': { from: { transform: 'translateX(0)' }, to: { transform: 'translateX(100%)' } },
        // Loading placeholders: a deeper, quicker fade than Tailwind's animate-pulse (1 -> 0.5 over 2 s).
        skeleton: { '0%, 100%': { opacity: '1' }, '50%': { opacity: '0.3' } },
      },
      animation: {
        'panel-in': 'panel-in 300ms cubic-bezier(0.32, 0.72, 0, 1) both',
        'panel-out': 'panel-out 220ms cubic-bezier(0.4, 0, 1, 1) both',
        skeleton: 'skeleton 1.2s ease-in-out infinite',
      },
    },
  },
  plugins: [require('@tailwindcss/typography')],
}
