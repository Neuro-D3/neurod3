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
      },
      animation: {
        'panel-in': 'panel-in 300ms cubic-bezier(0.32, 0.72, 0, 1) both',
        'panel-out': 'panel-out 220ms cubic-bezier(0.4, 0, 1, 1) both',
      },
    },
  },
  plugins: [require('@tailwindcss/typography')],
}


