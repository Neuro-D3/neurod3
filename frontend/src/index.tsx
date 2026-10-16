import React from 'react';
import ReactDOM from 'react-dom/client';
// JetBrains Mono for ids and code, served from our own origin (Latin, 400 and 500 only).
import '@fontsource/jetbrains-mono/latin-400.css';
import '@fontsource/jetbrains-mono/latin-500.css';
import './index.css';
import App from './App';

const root = ReactDOM.createRoot(
  document.getElementById('root') as HTMLElement
);
root.render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
);


