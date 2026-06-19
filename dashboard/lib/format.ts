// Small formatting helpers shared across pages.

export function shortId(id: string, length = 8): string {
  if (!id) return '';
  return id.length > length ? id.slice(0, length) : id;
}

export function formatDateTime(value: string): string {
  if (!value) return '-';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
}

export function pillClass(status: string): string {
  const normalized = (status || '').toLowerCase();
  return `pill pill-${normalized}`;
}
