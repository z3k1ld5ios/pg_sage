export function StatusDot({ connected, error }) {
  const color = !connected
    ? 'var(--red)'
    : error ? 'var(--yellow)' : 'var(--green)'
  const label = !connected ? 'Disconnected' : error ? 'Warning' : 'Connected'
  return (
    <span className="inline-flex items-center gap-1.5"
      role="status" aria-label={label}>
      <span className="inline-block w-2.5 h-2.5 rounded-full"
        style={{ background: color }} />
      <span className="text-xs" style={{ color }}>
        {label}
      </span>
    </span>
  )
}
