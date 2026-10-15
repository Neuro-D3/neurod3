import { useCallback, useEffect, useState } from 'react';

/**
 * Light or dark theme for the whole site. The choice is saved in
 * localStorage under THEME_KEY; public/index.html applies it before the first
 * paint so a dark page doesn't flash light. Light is the default.
 */
export type Theme = 'light' | 'dark';

export const THEME_KEY = 'd3-theme';

/** The saved theme, or light when nothing usable is saved or storage is blocked. */
export function readStoredTheme(storage: Pick<Storage, 'getItem'> | null | undefined): Theme {
  try {
    return storage?.getItem(THEME_KEY) === 'dark' ? 'dark' : 'light';
  } catch {
    return 'light';
  }
}

function browserStorage(): Storage | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

/** The current theme and a toggle; keeps the `dark` class on <html> and the saved value in step. */
export function useTheme(): [Theme, () => void] {
  const [theme, setTheme] = useState<Theme>(() => readStoredTheme(browserStorage()));

  useEffect(() => {
    document.documentElement.classList.toggle('dark', theme === 'dark');
    try {
      browserStorage()?.setItem(THEME_KEY, theme);
    } catch {
      // Storage blocked: the theme still applies for this visit.
    }
  }, [theme]);

  const toggle = useCallback(() => setTheme((t) => (t === 'dark' ? 'light' : 'dark')), []);
  return [theme, toggle];
}
