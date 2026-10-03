/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
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


