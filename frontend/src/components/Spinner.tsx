/** The small circular loading indicator used on the home and Paper Mapping tabs. */
export default function Spinner({ darkMode = false }: { darkMode?: boolean }) {
  return (
    <span
      aria-hidden="true"
      className={`inline-block h-4 w-4 shrink-0 animate-spin rounded-full border-2 motion-reduce:animate-none ${
        darkMode ? 'border-white/25 border-t-white' : 'border-slate-300 border-t-slate-700'
      }`}
    />
  );
}
