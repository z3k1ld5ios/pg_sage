import { useState } from 'react'
import { useAPI } from '../hooks/useAPI'
import { SQLBlock } from '../components/SQLBlock'
import { DataTable } from '../components/DataTable'
import { TimeAgo } from '../components/TimeAgo'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { ErrorBanner } from '../components/ErrorBanner'
import { EmptyState } from '../components/EmptyState'

export function Actions({ database, user }) {
  const [tab, setTab] = useState('executed')
  const dbParam = database && database !== 'all'
    ? `?database=${database}` : ''

  const { data, loading, error, refetch } =
    useAPI(`/api/v1/actions${dbParam}`)
  const {
    data: pendingData,
    loading: pendingLoading,
    error: pendingError,
    refetch: pendingRefetch,
  } = useAPI('/api/v1/actions/pending')

  if (tab === 'executed') {
    return (
      <div className="space-y-4">
        <TabBar tab={tab} setTab={setTab}
          pendingCount={pendingData?.total || 0} />
        <ExecutedTab data={data} loading={loading}
          error={error} refetch={refetch} />
      </div>
    )
  }

  return (
    <div className="space-y-4">
      <TabBar tab={tab} setTab={setTab}
        pendingCount={pendingData?.total || 0} />
      <PendingTab data={pendingData}
        loading={pendingLoading}
        error={pendingError}
        refetch={pendingRefetch} user={user} />
    </div>
  )
}

function TabBar({ tab, setTab, pendingCount }) {
  const tabs = [
    { key: 'executed', label: 'Executed' },
    { key: 'pending', label: 'Pending Approval' },
  ]

  return (
    <div className="flex gap-2">
      {tabs.map(t => (
        <button key={t.key} onClick={() => setTab(t.key)}
          data-testid={`actions-tab-${t.key}`}
          className="px-3 py-1.5 rounded text-sm"
          style={{
            background: tab === t.key
              ? 'var(--accent)' : 'var(--bg-card)',
            color: tab === t.key
              ? '#fff' : 'var(--text-secondary)',
            border: '1px solid var(--border)',
          }}>
          {t.label}
          {t.key === 'pending' && pendingCount > 0 && (
            <span className="ml-1.5 px-1.5 py-0.5 rounded-full text-xs"
              style={{
                background: 'var(--red)',
                color: '#fff',
              }}>
              {pendingCount}
            </span>
          )}
        </button>
      ))}
    </div>
  )
}

function ExecutedTab({ data, loading, error, refetch }) {
  if (loading) return <LoadingSpinner />
  if (error) return <ErrorBanner message={error}
    onRetry={refetch} />

  const actions = data?.actions || []

  const columns = [
    { key: 'action_type', label: 'Type' },
    {
      key: 'summary', label: 'Summary',
      render: r => {
        const t = (r.action_type || '').toLowerCase()
        if (t.includes('index') && r.outcome === 'success') {
          return 'Index operation completed successfully'
        }
        if (t.includes('vacuum')) return 'Table maintenance completed'
        if (t.includes('analyze')) return 'Statistics updated'
        const raw = r.action_type || ''
        return raw.charAt(0).toUpperCase() + raw.slice(1)
      },
    },
    {
      key: 'outcome', label: 'Outcome',
      render: r => (
        <span style={{
          color: r.outcome === 'success'
            ? 'var(--green)' : 'var(--red)',
        }}>
          {r.outcome}
        </span>
      ),
    },
    { key: 'database_name', label: 'Database' },
    {
      key: 'executed_at', label: 'When',
      render: r => <TimeAgo timestamp={r.executed_at} />,
    },
  ]

  if (actions.length === 0) {
    return <EmptyState message="No actions have been executed yet. Actions will appear here as pg_sage works on your databases." />
  }

  return (
    <DataTable data-testid="executed-actions-table"
      columns={columns} rows={actions} expandable
      renderExpanded={row => (
        <div className="space-y-3">
          <div>
            <div className="text-xs font-medium mb-1"
              style={{ color: 'var(--text-secondary)' }}>
              SQL Executed
            </div>
            <SQLBlock sql={row.sql_executed} />
          </div>
          {row.rollback_sql && (
            <div>
              <div className="text-xs font-medium mb-1"
                style={{ color: 'var(--text-secondary)' }}>
                Rollback SQL
              </div>
              <SQLBlock sql={row.rollback_sql} />
            </div>
          )}
        </div>
      )}
    />
  )
}

function PendingTab({
  data, loading, error, refetch, user,
}) {
  const [rejectId, setRejectId] = useState(null)
  const [rejectReason, setRejectReason] = useState('')
  const [actionMsg, setActionMsg] = useState(null)

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorBanner message={error}
    onRetry={refetch} />

  const actions = data?.pending || []

  async function handleApprove(id) {
    setActionMsg(null)
    try {
      const res = await fetch(
        `/api/v1/actions/${id}/approve`,
        { method: 'POST', credentials: 'include' },
      )
      const json = await res.json()
      if (json.ok) {
        setActionMsg({ type: 'success',
          text: `Action ${id} approved and executed` })
      } else {
        setActionMsg({ type: 'error',
          text: json.error || 'Approve failed' })
      }
      refetch()
    } catch (err) {
      setActionMsg({ type: 'error', text: err.message })
    }
  }

  async function handleReject(id) {
    setActionMsg(null)
    try {
      const res = await fetch(
        `/api/v1/actions/${id}/reject`,
        {
          method: 'POST',
          credentials: 'include',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ reason: rejectReason }),
        },
      )
      const json = await res.json()
      if (json.ok) {
        setActionMsg({ type: 'success',
          text: `Action ${id} rejected` })
      } else {
        setActionMsg({ type: 'error',
          text: json.error || 'Reject failed' })
      }
      setRejectId(null)
      setRejectReason('')
      refetch()
    } catch (err) {
      setActionMsg({ type: 'error', text: err.message })
    }
  }

  const columns = [
    { key: 'action_risk', label: 'Risk',
      render: r => {
        const riskMap = {
          safe: { label: 'Low Risk', color: 'var(--green)' },
          moderate: {
            label: 'Moderate Risk', color: 'var(--yellow)',
          },
          high: { label: 'High Risk', color: 'var(--red)' },
        }
        const info = riskMap[r.action_risk] || {
          label: r.action_risk,
          color: 'var(--text-secondary)',
        }
        return (
          <span style={{ color: info.color }}>
            {info.label}
          </span>
        )
      },
    },
    { key: 'finding_id', label: 'Finding' },
    {
      key: 'proposed_at', label: 'Proposed',
      render: r => <TimeAgo timestamp={r.proposed_at} />,
    },
    {
      key: 'actions', label: '',
      render: r => (
        <div className="flex gap-2">
          <button onClick={() => handleApprove(r.id)}
            data-testid="approve-button"
            className="px-2 py-1 rounded text-xs"
            style={{
              background: 'var(--green)',
              color: '#fff',
            }}>
            Approve
          </button>
          <button
            onClick={() => setRejectId(
              rejectId === r.id ? null : r.id)}
            data-testid="reject-button"
            className="px-2 py-1 rounded text-xs"
            style={{
              background: 'var(--red)',
              color: '#fff',
            }}>
            Reject
          </button>
        </div>
      ),
    },
  ]

  if (actions.length === 0) {
    return <EmptyState message="No actions waiting for approval. When pg_sage identifies improvements that need your OK, they'll appear here." />
  }

  return (
    <div className="space-y-3">
      <p data-testid="pending-help-text"
        className="text-sm"
        style={{ color: 'var(--text-secondary)' }}>
        These actions are waiting for your review. Approve to
        execute, or reject with a reason.
      </p>
      {actionMsg && (
        <div className="p-2 rounded text-sm"
          style={{
            background: actionMsg.type === 'success'
              ? 'var(--green)' : 'var(--red)',
            color: '#fff',
            opacity: 0.9,
          }}>
          {actionMsg.text}
        </div>
      )}
      <DataTable data-testid="pending-actions-table"
        columns={columns} rows={actions} expandable
        renderExpanded={row => (
          <div className="space-y-3">
            <div>
              <div className="text-xs font-medium mb-1"
                style={{ color: 'var(--text-secondary)' }}>
                Proposed SQL
              </div>
              <SQLBlock sql={row.proposed_sql} />
            </div>
            {row.rollback_sql && (
              <div>
                <div className="text-xs font-medium mb-1"
                  style={{ color: 'var(--text-secondary)' }}>
                  Rollback SQL
                </div>
                <SQLBlock sql={row.rollback_sql} />
              </div>
            )}
            {rejectId === row.id && (
              <div className="flex gap-2 items-center">
                <input
                  value={rejectReason}
                  onChange={e =>
                    setRejectReason(e.target.value)}
                  placeholder="Reason for rejection..."
                  className="px-2 py-1 rounded text-sm flex-1"
                  style={{
                    background: 'var(--bg-primary)',
                    color: 'var(--text-primary)',
                    border: '1px solid var(--border)',
                  }}
                />
                <button
                  onClick={() => handleReject(row.id)}
                  className="px-2 py-1 rounded text-xs"
                  style={{
                    background: 'var(--red)',
                    color: '#fff',
                  }}>
                  Confirm Reject
                </button>
              </div>
            )}
          </div>
        )}
      />
    </div>
  )
}
