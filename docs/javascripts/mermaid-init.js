document.addEventListener('DOMContentLoaded', async () => {
  if (!window.mermaid) return;
  const scheme = document.body.getAttribute('data-md-color-scheme') || 'default';
  window.mermaid.initialize({
    startOnLoad: false,
    theme: scheme === 'slate' ? 'dark' : 'default',
    securityLevel: 'loose',
  });
  await window.mermaid.run({ querySelector: 'div.mermaid' });
});
