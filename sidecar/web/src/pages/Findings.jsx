import { useState } from 'react'
import { useAPI } from '../hooks/useAPI'
import { SeverityBadge } from '../components/SeverityBadge'
import { SQLBlock } from '../components/SQLBlock'
import { DataTable } from '../components/DataTable'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { ErrorBanner } from '../components/ErrorBanner'
import { EmptyState } from '../components/EmptyState'

const emptyMessages = {
  open: 'No open recommendations. Your databases look good!',
  suppressed: 'No suppressed recommendations.',
  resolved:
    'No resolved recommendations yet.'
    + ' Recommendations move here after you act on them.',
}

function formatDetailKey(key) {
  return key.replace(/_/g, ' ').replace(/^./, c => c.toUpperCase())
}

function formatDetailValue(value) {
  if (typeof value === 'boolean') return value ? 'Yes' : 'No'
  if (typeof value === 'number' && !Number.isInteger(value)) {
    return Math.round(value * 100) / 100
  }
  if (value === null || value === undefined) return '-'
  if (typeof value === 'object') return JSON.stringify(value)
  return String(value)
}

const riskStyles = {
  safe: {
    background: 'var(--green)',
    label: 'Low Risk',
  },
  moderate: {
    background: 'var(--yellow, #eab308)',
    label: 'Moderate Risk',
  },
  high: {
    background: 'var(--red)',
    label: 'High Risk \u2014 Review Carefully',
  },
}

export function Findings({ database, user }) {
  const [status, setStatus] = useState('open')
  const [severity, setSeverity] = useState('')
  const dbParam = database && database !== 'all'
    ? `&database=${database}` : ''
  const sevParam = severity ? `&severity=${severity}` : ''
  const url =
    `/api/v1/findings?status=${status}${dbParam}${sevParam}&limit=50`
  const { data, loading, error, refetch } = useAPI(url)

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorBanner message={error} onRetry={refetch} />

  const findings = data?.findings || []
  const canAct = user?.role === 'admin'
    || user?.role === 'operator'

  const columns = [
    {
      key: 'severity', label: 'Severity',
      render: r => <SeverityBadge severity={r.severity} />,
    },
    { key: 'category', label: 'Category' },
    { key: 'title', label: 'Title' },
    { key: 'database_name', label: 'Database' },
    { key: 'occurrence_count', label: 'Count' },
  ]

  return (
    <div className="space-y-4">
      <div className="flex gap-2">
        {['open', 'suppressed', 'resolved'].map(s => (
          <button key={s} onClick={() => setStatus(s)}
            className="px-3 py-1.5 rounded text-sm"
            style={{
              background: status === s
                ? 'var(--accent)' : 'var(--bg-card)',
              color: status === s ? '#fff' : 'var(--text-secondary)',
              border: '1px solid var(--border)',
            }}>
            {s.charAt(0).toUpperCase() + s.slice(1)}
          </button>
        ))}
        <select value={severity}
          onChange={e => setSeverity(e.target.value)}
          data-testid="severity-filter"
          className="px-3 py-1.5 rounded text-sm ml-auto"
          style={{
            background: 'var(--bg-card)',
            color: 'var(--text-primary)',
            border: '1px solid var(--border)',
          }}>
          <option value="">All severities</option>
          <option value="critical">Critical</option>
          <option value="warning">Warning</option>
          <option value="info">Info</option>
        </select>
      </div>

      {findings.length === 0 ? (
        <EmptyState message={emptyMessages[status]} />
      ) : (
        <DataTable data-testid="findings-table"
          columns={columns} rows={findings} expandable
          renderExpanded={row => (
            <FindingDetail row={row} canAct={canAct}
              onActionDone={refetch} />
          )}
        />
      )}

      <div className="text-xs"
        data-testid="findings-count"
        style={{ color: 'var(--text-secondary)' }}>
        {data?.total || 0} total recommendations
      </div>
    </div>
  )
}

function FindingDetail({ row, canAct, onActionDone }) {
  const [showModal, setShowModal] = useState(false)
  const [executing, setExecuting] = useState(false)
  const [result, setResult] = useState(null)
  const [showRawJson, setShowRawJson] = useState(false)

  async function handleExecute() {
    setExecuting(true)
    setResult(null)
    try {
      const res = await fetch('/api/v1/actions/execute', {
        method: 'POST',
        credentials: 'include',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          finding_id: parseInt(row.id, 10),
          sql: row.recommended_sql,
        }),
      })
      const json = await res.json()
      if (json.ok) {
        setResult({
          type: 'success',
          text: `Executed (action log #${json.action_log_id})`,
        })
        if (onActionDone) onActionDone()
      } else {
        setResult({
          type: 'error',
          text: json.error || 'Execution failed',
        })
      }
    } catch (err) {
      setResult({ type: 'error', text: err.message })
    } finally {
      setExecuting(false)
      setShowModal(false)
    }
  }

  const risk = row.action_risk && riskStyles[row.action_risk]

  return (
    <div className="space-y-3">
      <p className="text-sm"
        style={{ color: 'var(--text-secondary)' }}>
        {row.recommendation}
      </p>
      {row.recommended_sql && (
        <div>
          <div className="text-xs font-medium mb-1"
            style={{ color: 'var(--text-secondary)' }}>
            Recommended SQL
          </div>
          <SQLBlock sql={row.recommended_sql} />
        </div>
      )}
      {row.detail && (
        <div>
          <div className="text-xs font-medium mb-1"
            style={{ color: 'var(--text-secondary)' }}>
            Detail
          </div>
          <div data-testid="detail-grid"
            className="grid gap-x-4 gap-y-1 text-xs p-2 rounded"
            style={{
              gridTemplateColumns: 'max-content 1fr',
              background: 'var(--bg-primary)',
            }}>
            {Object.entries(row.detail).map(([k, v]) => (
              <div key={k} className="contents">
                <span className="font-medium"
                  style={{ color: 'var(--text-secondary)' }}>
                  {formatDetailKey(k)}
                </span>
                <span style={{ color: 'var(--text-primary)' }}>
                  {formatDetailValue(v)}
                </span>
              </div>
            ))}
          </div>
          <button
            data-testid="show-raw-json"
            onClick={() => setShowRawJson(prev => !prev)}
            className="text-xs mt-1 px-2 py-0.5 rounded"
            style={{
              background: 'transparent',
              color: 'var(--text-secondary)',
              border: '1px solid var(--border)',
              cursor: 'pointer',
            }}>
            {showRawJson ? 'Hide raw JSON' : 'Show raw JSON'}
          </button>
          {showRawJson && (
            <pre className="text-xs p-2 mt-1 rounded overflow-auto"
              style={{
                background: 'var(--bg-primary)',
                color: 'var(--text-secondary)',
              }}>
              {JSON.stringify(row.detail, null, 2)}
            </pre>
          )}
        </div>
      )}

      {result && (
        <div className="p-2 rounded text-sm"
          style={{
            background: result.type === 'success'
              ? 'var(--green)' : 'var(--red)',
            color: '#fff',
            opacity: 0.9,
          }}>
          {result.text}
        </div>
      )}

      {canAct && row.recommended_sql && row.status === 'open'
        && !row.acted_on_at && (
        <div>
          {risk && (
            <span className="inline-block text-xs font-medium
              px-2 py-0.5 rounded mr-2 mb-2"
              style={{
                background: risk.background,
                color: '#fff',
              }}>
              {risk.label}
            </span>
          )}
          {showModal ? (
            <div className="p-3 rounded space-y-2"
              style={{
                background: 'var(--bg-primary)',
                border: '1px solid var(--border)',
              }}>
              <div className="text-sm font-medium"
                style={{ color: 'var(--text-primary)' }}>
                Confirm execution:
              </div>
              <SQLBlock sql={row.recommended_sql} />
              <div className="flex gap-2">
                <button onClick={handleExecute}
                  disabled={executing}
                  className="px-3 py-1.5 rounded text-sm"
                  style={{
                    background: 'var(--green)',
                    color: '#fff',
                    opacity: executing ? 0.5 : 1,
                  }}>
                  {executing ? 'Executing...' : 'Execute'}
                </button>
                <button
                  onClick={() => setShowModal(false)}
                  className="px-3 py-1.5 rounded text-sm"
                  style={{
                    background: 'var(--bg-card)',
                    color: 'var(--text-secondary)',
                    border: '1px solid var(--border)',
                  }}>
                  Cancel
                </button>
              </div>
            </div>
          ) : (
            <button
              onClick={() => setShowModal(true)}
              className="px-3 py-1.5 rounded text-sm"
              style={{
                background: 'var(--accent)',
                color: '#fff',
              }}>
              Take Action
            </button>
          )}
        </div>
      )}
    </div>
  )
}
