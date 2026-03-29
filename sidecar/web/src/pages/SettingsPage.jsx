import { useState, useEffect, useCallback } from 'react'
import { useAPI } from '../hooks/useAPI'
import { LoadingSpinner } from '../components/LoadingSpinner'
import { ErrorBanner } from '../components/ErrorBanner'
import {
  ShieldAlert, Play, Save, RotateCcw, Check, X,
} from 'lucide-react'

const ADVANCED_TABS = [
  'General', 'Collector', 'Analyzer', 'Trust & Safety',
  'LLM', 'Alerting', 'Retention',
]

const SIMPLE_TABS = ['General', 'Monitoring', 'AI & Alerts']

function getInitialMode() {
  try {
    const stored = localStorage.getItem('pg_sage_settings_mode')
    if (stored === 'advanced' || stored === 'simple') return stored
  } catch {
    // localStorage unavailable
  }
  return 'simple'
}

export function SettingsPage({ database }) {
  const { data, loading, error, refetch } = useAPI(
    '/api/v1/config/global', 0
  )
  const [mode, setMode] = useState(getInitialMode)
  const [tab, setTab] = useState('General')
  const [edits, setEdits] = useState({})
  const [saving, setSaving] = useState(false)
  const [feedback, setFeedback] = useState(null)
  const [stopping, setStopping] = useState(false)

  const tabs = mode === 'simple' ? SIMPLE_TABS : ADVANCED_TABS

  useEffect(() => { setEdits({}); setFeedback(null) }, [tab])

  useEffect(() => {
    if (!tabs.includes(tab)) setTab('General')
  }, [mode, tabs, tab])

  const toggleMode = () => {
    const next = mode === 'simple' ? 'advanced' : 'simple'
    setMode(next)
    try { localStorage.setItem('pg_sage_settings_mode', next) } catch {
      // localStorage unavailable
    }
  }

  const cfg = data?.config || {}

  const getVal = useCallback((key) => {
    if (key in edits) return edits[key]
    const entry = cfg[key]
    return entry?.value ?? ''
  }, [cfg, edits])

  const getSource = useCallback((key) => {
    if (key in edits) return 'modified'
    return cfg[key]?.source ?? 'default'
  }, [cfg, edits])

  const setVal = (key, val) => {
    setEdits(prev => ({ ...prev, [key]: val }))
    setFeedback(null)
  }

  const resetField = (key) => {
    setEdits(prev => {
      const next = { ...prev }
      delete next[key]
      return next
    })
  }

  const saveChanges = async () => {
    if (Object.keys(edits).length === 0) return
    setSaving(true)
    setFeedback(null)
    try {
      const res = await fetch('/api/v1/config/global', {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        credentials: 'include',
        body: JSON.stringify(edits),
      })
      if (!res.ok) {
        const err = await res.json()
        setFeedback({ type: 'error', msg: err.error || 'Save failed' })
        return
      }
      setFeedback({ type: 'success', msg: 'Settings saved' })
      setEdits({})
      refetch()
    } catch (e) {
      setFeedback({ type: 'error', msg: e.message })
    } finally {
      setSaving(false)
    }
  }

  if (loading) return <LoadingSpinner />
  if (error) return <ErrorBanner message={error} onRetry={refetch} />

  const fieldProps = { getVal, setVal, getSource, resetField }
  const isGeneralTab = tab === 'General'
  const hasEdits = Object.keys(edits).length > 0

  return (
    <div className="space-y-4 max-w-3xl">
      <div className="flex items-center justify-between">
        <TabBar tabs={tabs} active={tab} onSelect={setTab} />
        <button
          data-testid="settings-mode-toggle"
          onClick={toggleMode}
          className="text-xs px-3 py-1 rounded whitespace-nowrap ml-4"
          style={{
            color: 'var(--text-secondary)',
            border: '1px solid var(--border)',
          }}
        >
          {mode === 'simple' ? 'Show Advanced' : 'Show Simple'}
        </button>
      </div>
      {feedback && <FeedbackBanner {...feedback} />}
      <div className="rounded p-5"
        style={{
          background: 'var(--bg-card)',
          border: '1px solid var(--border)',
        }}>
        {mode === 'simple' ? (
          <SimpleContent
            tab={tab}
            data={data}
            database={database}
            stopping={stopping}
            setStopping={setStopping}
            refetch={refetch}
            {...fieldProps}
          />
        ) : (
          <AdvancedContent
            tab={tab}
            data={data}
            database={database}
            stopping={stopping}
            setStopping={setStopping}
            refetch={refetch}
            {...fieldProps}
          />
        )}
      </div>
      {!isGeneralTab && hasEdits && (
        <div className="flex gap-3">
          <button onClick={saveChanges} disabled={saving}
            data-testid="settings-save"
            className="flex items-center gap-2 px-4 py-2 rounded text-sm font-medium"
            style={{ background: 'var(--accent)', color: '#fff' }}>
            <Save size={16} />
            {saving ? 'Saving...' : 'Save Changes'}
          </button>
          <button onClick={() => setEdits({})}
            data-testid="settings-discard"
            className="flex items-center gap-2 px-4 py-2 rounded text-sm"
            style={{
              color: 'var(--text-secondary)',
              border: '1px solid var(--border)',
            }}>
            <RotateCcw size={16} /> Discard
          </button>
        </div>
      )}
    </div>
  )
}

/* ---------- Simple mode content ---------- */

function SimpleContent({
  tab, data, database, stopping, setStopping, refetch,
  getVal, setVal, getSource, resetField,
}) {
  const fieldProps = { getVal, setVal, getSource, resetField }
  if (tab === 'General') {
    return (
      <GeneralTab
        mode={data?.mode} databases={data?.databases}
        database={database} stopping={stopping}
        setStopping={setStopping} refetch={refetch}
      />
    )
  }
  if (tab === 'Monitoring') {
    return <SimpleMonitoringTab {...fieldProps} />
  }
  if (tab === 'AI & Alerts') {
    return <SimpleAIAlertsTab {...fieldProps} />
  }
  return null
}

/* ---------- Advanced mode content ---------- */

function AdvancedContent({
  tab, data, database, stopping, setStopping, refetch,
  getVal, setVal, getSource, resetField,
}) {
  const fieldProps = { getVal, setVal, getSource, resetField }
  if (tab === 'General') {
    return (
      <GeneralTab
        mode={data?.mode} databases={data?.databases}
        database={database} stopping={stopping}
        setStopping={setStopping} refetch={refetch}
      />
    )
  }
  if (tab === 'Collector') return <CollectorTab {...fieldProps} />
  if (tab === 'Analyzer') return <AnalyzerTab {...fieldProps} />
  if (tab === 'Trust & Safety') return <TrustSafetyTab {...fieldProps} />
  if (tab === 'LLM') return <LLMTab {...fieldProps} />
  if (tab === 'Alerting') return <AlertingTab {...fieldProps} />
  if (tab === 'Retention') return <RetentionTab {...fieldProps} />
  return null
}

/* ---------- Simple mode: Monitoring tab ---------- */

function SimpleMonitoringTab(props) {
  const trustOptions = [
    { value: 'observation', label: 'Observation - Monitor only, no actions' },
    { value: 'advisory', label: 'Advisory - Safe actions only' },
    { value: 'autonomous', label: 'Autonomous - Safe + moderate actions' },
  ]
  const execOptions = [
    { value: 'auto', label: 'Auto - Execute without approval' },
    { value: 'approval', label: 'Approval - Require manual approval' },
    { value: 'manual', label: 'Manual - All actions manual' },
  ]
  return (
    <div className="space-y-6">
      <div>
        <SectionHeading>How pg_sage monitors</SectionHeading>
        <Field
          label="Collector Interval (seconds)"
          configKey="collector.interval_seconds"
          help="How often pg_sage checks your database health. Lower = more responsive but slightly more overhead."
          {...props}
        />
        <Field
          label="Slow Query Threshold (ms)"
          configKey="analyzer.slow_query_threshold_ms"
          help="Queries taking longer than this are flagged for review. 1000ms is a good default."
          {...props}
        />
        <Field
          label="Unused Index Window (days)"
          configKey="analyzer.unused_index_window_days"
          help="How many days an index must go unused before pg_sage flags it. A longer window avoids false positives from seasonal workloads."
          {...props}
        />
      </div>
      <div>
        <SectionHeading>Safety controls</SectionHeading>
        <Field
          label="Trust Level"
          configKey="trust.level"
          type="select"
          options={trustOptions}
          help="Controls how much autonomy pg_sage has. Start with Observation to just watch, then graduate to Advisory for safe recommendations."
          {...props}
        />
        <Field
          label="Execution Mode"
          configKey="execution_mode"
          type="select"
          options={execOptions}
          help="How pg_sage handles approved actions. Auto executes immediately, Approval waits for your OK."
          {...props}
        />
        <Field
          label="CPU Ceiling (%)"
          configKey="safety.cpu_ceiling_pct"
          help="pg_sage pauses all automated actions when CPU usage exceeds this threshold. Protects your database during peak load."
          {...props}
        />
      </div>
    </div>
  )
}

/* ---------- Simple mode: AI & Alerts tab ---------- */

function SimpleAIAlertsTab(props) {
  return (
    <div className="space-y-6">
      <div>
        <SectionHeading>AI Analysis</SectionHeading>
        <Field
          label="LLM Enabled"
          configKey="llm.enabled"
          type="toggle"
          help="Enable AI-powered analysis for deeper insights and natural language health briefings."
          {...props}
        />
        <Field
          label="Endpoint URL"
          configKey="llm.endpoint"
          type="text"
          help="The URL of your AI provider. Works with any OpenAI-compatible API (OpenAI, Gemini, Groq, Ollama, etc)."
          {...props}
        />
        <ModelField
          help="Which AI model to use for analysis. Discover available models or type a name manually."
          {...props}
        />
        <Field
          label="API Key"
          configKey="llm.api_key"
          type="password"
          help="Your AI provider's API key. Stored securely and never logged."
          {...props}
        />
      </div>
      <div>
        <SectionHeading>Alerting</SectionHeading>
        <Field
          label="Alerting Enabled"
          configKey="alerting.enabled"
          type="toggle"
          help="Turn on notifications so pg_sage can alert you when it finds issues or takes actions."
          {...props}
        />
        <Field
          label="Slack Webhook URL"
          configKey="alerting.slack_webhook_url"
          type="text"
          help="Paste your Slack incoming webhook URL to receive alerts in a Slack channel."
          {...props}
        />
        <Field
          label="Check Interval (seconds)"
          configKey="alerting.check_interval_seconds"
          help="How often pg_sage checks for new alerts to send. Lower values mean faster notifications but more processing."
          {...props}
        />
      </div>
    </div>
  )
}

/* ---------- Section heading for Simple mode ---------- */

function SectionHeading({ children }) {
  return (
    <h3 className="text-sm font-medium mb-3"
      style={{ color: 'var(--text-secondary)' }}>
      {children}
    </h3>
  )
}

/* ---------- Shared UI components ---------- */

function TabBar({ tabs, active, onSelect }) {
  return (
    <div className="flex gap-1 border-b pb-0 min-w-0 overflow-x-auto"
      style={{ borderColor: 'var(--border)' }}>
      {tabs.map(t => (
        <button key={t} onClick={() => onSelect(t)}
          data-testid={`settings-tab-${t.toLowerCase().replace(/\s+&\s+/g, '-').replace(/\s+/g, '-')}`}
          className="px-4 py-2 text-sm rounded-t whitespace-nowrap"
          style={{
            color: active === t ? 'var(--accent)' : 'var(--text-secondary)',
            borderBottom: active === t
              ? '2px solid var(--accent)'
              : '2px solid transparent',
            background: active === t ? 'var(--bg-card)' : 'transparent',
          }}>
          {t}
        </button>
      ))}
    </div>
  )
}

function FeedbackBanner({ type, msg }) {
  const color = type === 'success' ? 'var(--green)' : 'var(--red)'
  const Icon = type === 'success' ? Check : X
  return (
    <div className="flex items-center gap-2 px-4 py-2 rounded text-sm"
      style={{
        background: type === 'success' ? '#0f2640' : '#3b1111',
        color,
      }}>
      <Icon size={16} /> {msg}
    </div>
  )
}

function SourceBadge({ source }) {
  const colors = {
    default: 'var(--text-secondary)',
    yaml: 'var(--blue)',
    override: 'var(--accent)',
    db_override: 'var(--green)',
    modified: 'var(--yellow)',
  }
  return (
    <span className="text-[10px] ml-2 px-1.5 py-0.5 rounded"
      style={{
        color: colors[source] || 'var(--text-secondary)',
        border: `1px solid ${colors[source] || 'var(--border)'}`,
      }}>
      {source}
    </span>
  )
}

function Field({
  label, configKey, type, getVal, setVal, getSource,
  resetField, options, help,
}) {
  const value = getVal(configKey)
  const source = getSource(configKey)
  return (
    <div className="flex items-center gap-3 py-2">
      <div className="w-64 flex-shrink-0">
        <label className="text-sm"
          style={{ color: 'var(--text-primary)' }}>
          {label}
          <SourceBadge source={source} />
        </label>
        {help && (
          <div className="text-[11px] mt-0.5"
            style={{ color: 'var(--text-secondary)' }}>
            {help}
          </div>
        )}
      </div>
      <div className="flex-1">
        {type === 'select' ? (
          <select value={value}
            onChange={e => setVal(configKey, e.target.value)}
            className="w-full px-3 py-1.5 rounded text-sm"
            style={{
              background: 'var(--bg-main)',
              color: 'var(--text-primary)',
              border: '1px solid var(--border)',
            }}>
            {options.map(o => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
        ) : type === 'toggle' ? (
          <button
            onClick={() => setVal(
              configKey,
              String(value) !== 'true' ? 'true' : 'false'
            )}
            className="px-3 py-1 rounded text-sm"
            style={{
              background: String(value) === 'true'
                ? 'var(--green)'
                : 'var(--bg-main)',
              color: String(value) === 'true'
                ? '#fff'
                : 'var(--text-secondary)',
              border: '1px solid var(--border)',
            }}>
            {String(value) === 'true' ? 'Enabled' : 'Disabled'}
          </button>
        ) : type === 'password' ? (
          <PasswordField value={value}
            onChange={v => setVal(configKey, v)} />
        ) : (
          <input type={type === 'float' ? 'number' : type || 'number'}
            step={type === 'float' ? '0.01' : '1'}
            value={value}
            onChange={e => setVal(configKey, e.target.value)}
            className="w-full px-3 py-1.5 rounded text-sm"
            style={{
              background: 'var(--bg-main)',
              color: 'var(--text-primary)',
              border: '1px solid var(--border)',
            }} />
        )}
      </div>
      {source !== 'default' && source !== 'yaml' && (
        <button onClick={() => resetField(configKey)} title="Reset"
          className="p-1 rounded"
          style={{ color: 'var(--text-secondary)' }}>
          <RotateCcw size={14} />
        </button>
      )}
    </div>
  )
}

function PasswordField({ value, onChange }) {
  const [show, setShow] = useState(false)
  return (
    <div className="flex gap-2">
      <input type={show ? 'text' : 'password'} value={value}
        onChange={e => onChange(e.target.value)}
        className="flex-1 px-3 py-1.5 rounded text-sm"
        style={{
          background: 'var(--bg-main)',
          color: 'var(--text-primary)',
          border: '1px solid var(--border)',
        }} />
      <button onClick={() => setShow(!show)}
        className="px-2 text-xs rounded"
        style={{
          color: 'var(--text-secondary)',
          border: '1px solid var(--border)',
        }}>
        {show ? 'Hide' : 'Show'}
      </button>
    </div>
  )
}

/* ---------- Advanced mode tab components (unchanged) ---------- */

function GeneralTab({
  mode, databases, database, stopping, setStopping, refetch,
}) {
  const emergencyStop = async () => {
    setStopping(true)
    const dbParam = database && database !== 'all'
      ? `?database=${database}` : ''
    try {
      await fetch(
        `/api/v1/emergency-stop${dbParam}`,
        { method: 'POST', credentials: 'include' },
      )
    } finally {
      setStopping(false)
      refetch()
    }
  }
  const resume = async () => {
    const dbParam = database && database !== 'all'
      ? `?database=${database}` : ''
    await fetch(
      `/api/v1/resume${dbParam}`,
      { method: 'POST', credentials: 'include' },
    )
    refetch()
  }
  return (
    <div className="space-y-4">
      <h3 className="text-sm font-medium"
        style={{ color: 'var(--text-secondary)' }}>
        System Info
      </h3>
      <div className="grid grid-cols-2 gap-3 text-sm">
        <div style={{ color: 'var(--text-secondary)' }}>Mode</div>
        <div style={{ color: 'var(--text-primary)' }}>
          {mode || 'unknown'}
        </div>
        <div style={{ color: 'var(--text-secondary)' }}>Databases</div>
        <div style={{ color: 'var(--text-primary)' }}>
          {databases ?? 0}
        </div>
      </div>
      <h3 className="text-sm font-medium mt-6"
        style={{ color: 'var(--text-secondary)' }}>
        Emergency Controls
      </h3>
      <div className="flex gap-3">
        <button onClick={emergencyStop} disabled={stopping}
          data-testid="emergency-stop-button"
          className="flex items-center gap-2 px-4 py-2 rounded text-sm font-medium"
          style={{
            background: '#3b1111',
            color: 'var(--red)',
            border: '1px solid var(--red)',
          }}>
          <ShieldAlert size={16} />
          {stopping ? 'Stopping...' : 'Emergency Stop'}
        </button>
        <button onClick={resume}
          data-testid="resume-button"
          className="flex items-center gap-2 px-4 py-2 rounded text-sm font-medium"
          style={{
            background: '#0f2640',
            color: 'var(--green)',
            border: '1px solid var(--green)',
          }}>
          <Play size={16} /> Resume
        </button>
      </div>
    </div>
  )
}

function CollectorTab(props) {
  return (
    <div>
      <h3 className="text-sm font-medium mb-3"
        style={{ color: 'var(--text-secondary)' }}>Collector</h3>
      <Field label="Interval (seconds)"
        configKey="collector.interval_seconds"
        help="How often to collect stats snapshots" {...props} />
      <Field label="Batch Size" configKey="collector.batch_size"
        help="Queries per collection batch" {...props} />
      <Field label="Max Queries" configKey="collector.max_queries"
        help="Maximum tracked queries" {...props} />
    </div>
  )
}

function AnalyzerTab(props) {
  return (
    <div>
      <h3 className="text-sm font-medium mb-3"
        style={{ color: 'var(--text-secondary)' }}>Analyzer</h3>
      <Field label="Interval (seconds)"
        configKey="analyzer.interval_seconds" {...props} />
      <Field label="Slow Query Threshold (ms)"
        configKey="analyzer.slow_query_threshold_ms" {...props} />
      <Field label="Seq Scan Min Rows"
        configKey="analyzer.seq_scan_min_rows" {...props} />
      <Field label="Unused Index Window (days)"
        configKey="analyzer.unused_index_window_days" {...props} />
      <Field label="Index Bloat Threshold (%)"
        configKey="analyzer.index_bloat_threshold_pct" {...props} />
      <Field label="Table Bloat Dead Tuple (%)"
        configKey="analyzer.table_bloat_dead_tuple_pct" {...props} />
      <Field label="Regression Threshold (%)"
        configKey="analyzer.regression_threshold_pct" {...props} />
      <Field label="Cache Hit Ratio Warning"
        configKey="analyzer.cache_hit_ratio_warning"
        type="float" help="0.0 to 1.0" {...props} />
    </div>
  )
}

function TrustSafetyTab(props) {
  const trustOptions = [
    { value: 'observation', label: 'Observation - Monitor only, no actions' },
    { value: 'advisory', label: 'Advisory - Safe actions only' },
    { value: 'autonomous', label: 'Autonomous - Safe + moderate actions' },
  ]
  const execOptions = [
    { value: 'auto', label: 'Auto - Execute without approval' },
    { value: 'approval', label: 'Approval - Require manual approval' },
    { value: 'manual', label: 'Manual - All actions manual' },
  ]
  return (
    <div className="space-y-6">
      <div>
        <h3 className="text-sm font-medium mb-3"
          style={{ color: 'var(--text-secondary)' }}>Trust</h3>
        <Field label="Trust Level" configKey="trust.level"
          type="select" options={trustOptions} {...props} />
        <Field label="Execution Mode" configKey="execution_mode"
          type="select" options={execOptions} {...props} />
        <Field label="Tier 3: Safe" configKey="trust.tier3_safe"
          type="toggle" {...props} />
        <Field label="Tier 3: Moderate" configKey="trust.tier3_moderate"
          type="toggle" {...props} />
        <Field label="Tier 3: High Risk" configKey="trust.tier3_high_risk"
          type="toggle" {...props} />
        <Field label="Maintenance Window (cron)"
          configKey="trust.maintenance_window" type="text"
          help="Cron expression, e.g. 0 2 * * 0" {...props} />
      </div>
      <div>
        <h3 className="text-sm font-medium mb-3"
          style={{ color: 'var(--text-secondary)' }}>Safety</h3>
        <Field label="CPU Ceiling (%)"
          configKey="safety.cpu_ceiling_pct" {...props} />
        <Field label="Query Timeout (ms)"
          configKey="safety.query_timeout_ms" {...props} />
        <Field label="DDL Timeout (seconds)"
          configKey="safety.ddl_timeout_seconds" {...props} />
        <Field label="Lock Timeout (ms)"
          configKey="safety.lock_timeout_ms" {...props} />
      </div>
      <div>
        <h3 className="text-sm font-medium mb-3"
          style={{ color: 'var(--text-secondary)' }}>Rollback</h3>
        <Field label="Rollback Threshold (%)"
          configKey="trust.rollback_threshold_pct" {...props} />
        <Field label="Rollback Window (minutes)"
          configKey="trust.rollback_window_minutes" {...props} />
        <Field label="Rollback Cooldown (days)"
          configKey="trust.rollback_cooldown_days" {...props} />
        <Field label="Cascade Cooldown (cycles)"
          configKey="trust.cascade_cooldown_cycles" {...props} />
      </div>
    </div>
  )
}

function ModelField({ getVal, setVal, getSource, resetField, help }) {
  const [models, setModels] = useState(null)
  const [loadingModels, setLoadingModels] = useState(false)
  const [modelError, setModelError] = useState(null)

  const discoverModels = async () => {
    setLoadingModels(true)
    setModelError(null)
    try {
      const res = await fetch('/api/v1/llm/models', {
        credentials: 'include',
      })
      if (!res.ok) {
        const err = await res.json().catch(() => ({}))
        throw new Error(err.error || `HTTP ${res.status}`)
      }
      const data = await res.json()
      setModels(data.models || [])
    } catch (e) {
      setModelError(e.message)
    } finally {
      setLoadingModels(false)
    }
  }

  const value = getVal('llm.model')
  const source = getSource('llm.model')

  if (models && models.length > 0) {
    const options = models.map(m => ({
      value: m.id,
      label: m.name || m.id,
    }))
    // Ensure current value is in the list as an option.
    if (value && !options.find(o => o.value === value)) {
      options.unshift({ value, label: `${value} (current)` })
    }
    return (
      <div className="flex items-center gap-3 py-2">
        <div className="w-64 flex-shrink-0">
          <label className="text-sm"
            style={{ color: 'var(--text-primary)' }}>
            Model
            <SourceBadge source={source} />
          </label>
          {help && (
            <div className="text-[11px] mt-0.5"
              style={{ color: 'var(--text-secondary)' }}>
              {help}
            </div>
          )}
        </div>
        <div className="flex-1 flex gap-2">
          <select value={value}
            onChange={e => setVal('llm.model', e.target.value)}
            className="flex-1 px-3 py-1.5 rounded text-sm"
            style={{
              background: 'var(--bg-main)',
              color: 'var(--text-primary)',
              border: '1px solid var(--border)',
            }}>
            {options.map(o => (
              <option key={o.value} value={o.value}>
                {o.label}
              </option>
            ))}
          </select>
          <button onClick={() => setModels(null)}
            className="px-2 text-xs rounded"
            title="Switch to manual input"
            style={{
              color: 'var(--text-secondary)',
              border: '1px solid var(--border)',
            }}>
            Manual
          </button>
        </div>
        {source !== 'default' && source !== 'yaml' && (
          <button onClick={() => resetField('llm.model')}
            title="Reset" className="p-1 rounded"
            style={{ color: 'var(--text-secondary)' }}>
            <RotateCcw size={14} />
          </button>
        )}
      </div>
    )
  }

  return (
    <div className="flex items-center gap-3 py-2">
      <div className="w-64 flex-shrink-0">
        <label className="text-sm"
          style={{ color: 'var(--text-primary)' }}>
          Model
          <SourceBadge source={source} />
        </label>
        {help && (
          <div className="text-[11px] mt-0.5"
            style={{ color: 'var(--text-secondary)' }}>
            {help}
          </div>
        )}
      </div>
      <div className="flex-1 flex gap-2">
        <input type="text" value={value}
          onChange={e => setVal('llm.model', e.target.value)}
          className="flex-1 px-3 py-1.5 rounded text-sm"
          style={{
            background: 'var(--bg-main)',
            color: 'var(--text-primary)',
            border: '1px solid var(--border)',
          }} />
        <button onClick={discoverModels}
          disabled={loadingModels}
          className="px-3 py-1.5 text-xs rounded whitespace-nowrap"
          style={{
            background: 'var(--accent)',
            color: '#fff',
            opacity: loadingModels ? 0.6 : 1,
          }}>
          {loadingModels ? 'Loading...' : 'Discover'}
        </button>
      </div>
      {modelError && (
        <span className="text-xs"
          style={{ color: 'var(--red)' }}>
          {modelError}
        </span>
      )}
      {source !== 'default' && source !== 'yaml' && (
        <button onClick={() => resetField('llm.model')}
          title="Reset" className="p-1 rounded"
          style={{ color: 'var(--text-secondary)' }}>
          <RotateCcw size={14} />
        </button>
      )}
    </div>
  )
}

function LLMTab(props) {
  return (
    <div>
      <h3 className="text-sm font-medium mb-3"
        style={{ color: 'var(--text-secondary)' }}>LLM</h3>
      <Field label="Enabled" configKey="llm.enabled"
        type="toggle" {...props} />
      <Field label="Endpoint URL" configKey="llm.endpoint"
        type="text" {...props} />
      <Field label="API Key" configKey="llm.api_key"
        type="password" {...props} />
      <ModelField {...props} />
      <Field label="Timeout (seconds)"
        configKey="llm.timeout_seconds" {...props} />
      <Field label="Token Budget (daily)"
        configKey="llm.token_budget_daily" {...props} />
      <Field label="Context Budget (tokens)"
        configKey="llm.context_budget_tokens" {...props} />
    </div>
  )
}

function AlertingTab(props) {
  return (
    <div>
      <h3 className="text-sm font-medium mb-3"
        style={{ color: 'var(--text-secondary)' }}>Alerting</h3>
      <Field label="Enabled" configKey="alerting.enabled"
        type="toggle" {...props} />
      <Field label="Slack Webhook URL"
        configKey="alerting.slack_webhook_url" type="text" {...props} />
      <Field label="PagerDuty Routing Key"
        configKey="alerting.pagerduty_routing_key"
        type="password" {...props} />
      <Field label="Check Interval (seconds)"
        configKey="alerting.check_interval_seconds" {...props} />
      <Field label="Cooldown (minutes)"
        configKey="alerting.cooldown_minutes" {...props} />
      <Field label="Quiet Hours Start"
        configKey="alerting.quiet_hours_start" type="text"
        help="HH:MM format, e.g. 22:00" {...props} />
      <Field label="Quiet Hours End"
        configKey="alerting.quiet_hours_end" type="text"
        help="HH:MM format, e.g. 06:00" {...props} />
    </div>
  )
}

function RetentionTab(props) {
  return (
    <div>
      <h3 className="text-sm font-medium mb-3"
        style={{ color: 'var(--text-secondary)' }}>Retention</h3>
      <Field label="Snapshots (days)"
        configKey="retention.snapshots_days" {...props} />
      <Field label="Findings (days)"
        configKey="retention.findings_days" {...props} />
      <Field label="Actions (days)"
        configKey="retention.actions_days" {...props} />
      <Field label="Explains (days)"
        configKey="retention.explains_days" {...props} />
    </div>
  )
}
