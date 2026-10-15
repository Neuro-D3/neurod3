/** @type {import('tailwindcss').Config} */

// NeuroD3 design tokens. The palette is the SfN 2026 dataset-card poster's:
// a navy masthead, a cool grey canvas, white cards with hairline borders, and
// two data hues (blue = reuse, orange = mention). Everything else is neutral.
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Masthead
        navy: { DEFAULT: '#021242', 700: '#0B1F5C', 600: '#16306F', muted: '#A9B6D3', sub: '#C9D3E6', tag: '#6F80A8', line: '#1E2F63' },
        // Text
        ink: { DEFAULT: '#16202A', 2: '#26313D', 3: '#33404D' },
        muted: '#5A6573',
        // Surfaces
        canvas: '#ECF0F4',
        panel: { DEFAULT: '#F4F6F9', head: '#F7F9FB' },
        line: { DEFAULT: '#D7DDE4', 2: '#E0E5EB', 3: '#EDF0F3', rule: '#C9D0D8', inner: '#E3E7EC' },
        // Links and focus
        link: { DEFAULT: '#1C5DB0', hover: '#0F3F80' },
        focus: '#2977D5',
        // Data hues
        reuse: { DEFAULT: '#2977D5', soft: '#86B3EA', tint: '#E3EEFB', ink: '#1C4E8F' },
        mention: { DEFAULT: '#EA6733', soft: '#F5B394', tint: '#FDEBE3', ink: '#8A3A14' },
        primary: { DEFAULT: '#021242', soft: '#8E9FC6', tint: '#E6EAF2', ink: '#021242' },
        bar: { DEFAULT: '#B8C1CA', soft: '#D5DBE2', hist: '#9FB4CE' },
        neither: { tint: '#EDF0F3', ink: '#33404D' },
        tan: { tint: '#F3F1EC', ink: '#5B4A1F' },
        warn: { DEFAULT: '#C98A0B', tint: '#FBF1DC', ink: '#6B4E0A' },
        danger: { DEFAULT: '#C0392B', tint: '#FBE4E4', ink: '#8A1F1F' },
        // Metadata chips
        chip: { bg: '#EEF4FC', border: '#CFE0F5', text: '#1C4E8F' },
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
