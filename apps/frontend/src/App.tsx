import { useState, useEffect } from 'react';
import { 
  Activity, 
  AlertTriangle, 
  CheckCircle2, 
  Database, 
  FileCode2, 
  Terminal, 
  ShieldAlert, 
  Play, 
  Check, 
  X, 
  RefreshCw, 
  UserCheck, 
  Server, 
  ArrowRight, 
  AlertCircle
} from 'lucide-react';

interface Incident {
  id: string;
  timestamp: string;
  task_id: string;
  fault_category: string;
  severity: 'high' | 'medium';
  status: 'pending_approval' | 'remediated' | 'escalated';
  evidence: string;
  root_cause: string;
  proposed_action: string;
}

interface LogLine {
  time: string;
  level: 'info' | 'warn' | 'error' | 'success';
  text: string;
}

function App() {
  const [activeTab, setActiveTab] = useState<'dashboard' | 'incidents' | 'logs'>('dashboard');
  const [pipelineStatus, setPipelineStatus] = useState<'healthy' | 'anomaly' | 'failed'>('healthy');
  const [activeFault, setActiveFault] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState<string | null>(null);

  // Sample data to start with
  const [incidents, setIncidents] = useState<Incident[]>([
    {
      id: "inc-1092",
      timestamp: "2026-08-20 12:44:12",
      task_id: "validate_quality",
      fault_category: "Volume Anomaly Spike",
      severity: "medium",
      status: "pending_approval",
      evidence: "Staged orders: 1,450 rows | Expected range: 240-360 rows | Status: Spike anomaly detected (+302% over baseline)",
      root_cause: "Duplicate file transfer from upstream scheduler retry.",
      proposed_action: "Execute idempotent DELETE on staged duplicates followed by target table rebuild."
    }
  ]);

  const [remediations, setRemediations] = useState([
    { id: "rem-091", time: "2026-08-20 10:15:00", fault: "Duplicate Ingestion", target: "fct_orders", method: "Idempotent MERGE", duration: "4.2s", status: "success" },
    { id: "rem-085", time: "2026-08-19 14:02:18", fault: "Staleness Warning", target: "fct_events", method: "Sensor Recrawl", duration: "8.5s", status: "success" }
  ]);

  const [consoleLogs, setConsoleLogs] = useState<LogLine[]>([
    { time: "17:56:01", level: "info", text: "Airflow LocalExecutor initialized." },
    { time: "17:56:03", level: "success", text: "Connected to local metadata database (Postgres 15)." },
    { time: "17:56:05", level: "info", text: "DAG 'self_healing_pipeline' scheduled for daily runs (02:00 UTC)." },
    { time: "17:56:10", level: "info", text: "Ingestion agent listening for task callbacks..." }
  ]);

  // Volume chart data (daily records loaded)
  const volumeData = [
    { day: "08-14", count: 310, status: "healthy" },
    { day: "08-15", count: 295, status: "healthy" },
    { day: "08-16", count: 320, status: "healthy" },
    { day: "08-17", count: 285, status: "healthy" },
    { day: "08-18", count: 300, status: "healthy" },
    { day: "08-19", count: 305, status: "healthy" },
    { day: "08-20", count: 1450, status: "anomaly" } // Ingested today's spike
  ];

  // Auto-scroll logs
  useEffect(() => {
    const el = document.getElementById('log-terminal');
    if (el) el.scrollTop = el.scrollHeight;
  }, [consoleLogs]);

  // Push new log entry helper
  const addLog = (level: LogLine['level'], text: string) => {
    const time = new Date().toTimeString().split(' ')[0];
    setConsoleLogs(prev => [...prev, { time, level, text }]);
  };

  // Inject a fault
  const handleInjectFault = (type: string) => {
    if (activeFault) return; // one at a time

    setActiveFault(type);
    setPipelineStatus(type === 'schema_drift' || type === 'null_spike' ? 'failed' : 'anomaly');

    let newIncident: Incident;
    
    if (type === 'schema_drift') {
      newIncident = {
        id: `inc-${Math.floor(1000 + Math.random() * 9000)}`,
        timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
        task_id: "validate_schema",
        fault_category: "Schema Drift",
        severity: "high",
        status: "pending_approval",
        evidence: "Field 'price' expected FLOAT, got STRING (e.g. '$14.99') | Total drifted rows: 100% of batch",
        root_cause: "Upstream API modification without notification (price field formatting).",
        proposed_action: "Quarantine batch, create schema evolution log, and escalate alert."
      };
      addLog("error", "Task validate_schema failed! Schema drift detected in products.csv.");
      addLog("warn", "Field 'price' contains type mismatch (expected FLOAT, got STRING).");
      addLog("info", "Triggering AI Diagnostic Agent callback...");
    } else if (type === 'null_spike') {
      newIncident = {
        id: `inc-${Math.floor(1000 + Math.random() * 9000)}`,
        timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
        task_id: "validate_quality",
        fault_category: "Null Spike",
        severity: "high",
        status: "pending_approval",
        evidence: "Field 'customer_id' contains 14.5% NULL values | Threshold SLA: 0.0%",
        root_cause: "Database extraction failure on client export.",
        proposed_action: "Halt transaction pipeline, quarantine table, and raise ticket."
      };
      addLog("error", "Task validate_quality failed! Null spike detected in orders_2026-08-20.csv.");
      addLog("info", "Running evidence collection... 42/290 rows contain null customer identifiers.");
    } else {
      newIncident = {
        id: `inc-${Math.floor(1000 + Math.random() * 9000)}`,
        timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
        task_id: "validate_quality",
        fault_category: "Referential Breakage",
        severity: "high",
        status: "pending_approval",
        evidence: "Staged orders reference customer_id 'C-9988' which does not exist in dim_customers.",
        root_cause: "Stale dimensions sync in upstream source.",
        proposed_action: "Quarantine missing reference records, generate surrogate logs, and load clean rows."
      };
      addLog("warn", "Referential breakage check failed! Missing keys in dimension tables.");
      addLog("info", "AI Agent diagnostics: 3 orders missing corresponding customer keys.");
    }

    setIncidents(prev => [newIncident, ...prev]);
    setActiveTab('incidents');
  };

  // Resolve / Approve incident
  const handleApproveRemediation = (id: string) => {
    const incident = incidents.find(i => i.id === id);
    if (!incident) return;

    setIsProcessing(id);
    addLog("info", `User approved remediation for incident ${id}.`);
    addLog("info", `Executing action: ${incident.proposed_action}`);

    setTimeout(() => {
      // Simulate Agent resolving in BigQuery
      addLog("success", "BigQuery session initialized with Service Account 'pipeline-intern-samri'.");
      addLog("info", `Running target cleanup query for ${incident.task_id}...`);
      
      setTimeout(() => {
        addLog("success", "Query execution complete. Quarantined drifted/failed records successfully.");
        addLog("info", "Running post-remediation validations...");
        
        setTimeout(() => {
          addLog("success", "Validation PASSED. All data quality indicators are healthy.");
          addLog("success", `Pipeline resumed. Incident ${id} resolved.`);

          // Move incident to resolved
          setIncidents(prev => prev.map(inc => inc.id === id ? { ...inc, status: "remediated" } : inc));
          setRemediations(prev => [
            {
              id: `rem-${Math.floor(100 + Math.random() * 900)}`,
              time: new Date().toISOString().replace('T', ' ').substring(0, 19),
              fault: incident.fault_category,
              target: incident.task_id === 'validate_schema' ? 'dim_products' : 'fct_orders',
              method: incident.fault_category.includes("Spike") ? "Idempotent Cleanse" : "Type Coercion",
              duration: "3.8s",
              status: "success"
            },
            ...prev
          ]);

          setIsProcessing(null);
          setActiveFault(null);
          setPipelineStatus('healthy');
          setActiveTab('dashboard');
        }, 1000);
      }, 1000);
    }, 1000);
  };

  // Decline incident
  const handleDeclineIncident = (id: string) => {
    setIncidents(prev => prev.filter(inc => inc.id !== id));
    addLog("warn", `Incident ${id} declined/dismissed by user. Escalation terminated.`);
    setActiveFault(null);
    setPipelineStatus('healthy');
  };

  return (
    <div className="app-container">
      {/* Sidebar Navigation */}
      <aside className="sidebar">
        <div className="top-part">
          <div className="logo-section">
            <Activity className="logo-icon" size={24} />
            <span className="logo-text">SelfHeal Agent</span>
          </div>

          <nav className="nav-links">
            <div 
              className={`nav-item ${activeTab === 'dashboard' ? 'active' : ''}`}
              onClick={() => setActiveTab('dashboard')}
            >
              <Server size={18} />
              Dashboard
            </div>
            <div 
              className={`nav-item ${activeTab === 'incidents' ? 'active' : ''}`}
              onClick={() => setActiveTab('incidents')}
            >
              <AlertTriangle size={18} />
              Incidents 
              {incidents.filter(i => i.status === 'pending_approval').length > 0 && (
                <span style={{ 
                  marginLeft: 'auto', 
                  backgroundColor: 'var(--failed)', 
                  color: '#000', 
                  fontSize: '11px', 
                  fontWeight: 'bold', 
                  padding: '2px 6px', 
                  borderRadius: '10px' 
                }}>
                  {incidents.filter(i => i.status === 'pending_approval').length}
                </span>
              )}
            </div>
            <div 
              className={`nav-item ${activeTab === 'logs' ? 'active' : ''}`}
              onClick={() => setActiveTab('logs')}
            >
              <Terminal size={18} />
              Console Logs
            </div>
          </nav>
        </div>

        <div className="sidebar-footer">
          <div>Cohort 2026 — Monorepo v1.0</div>
          <div style={{ marginTop: '4px', fontSize: '10px', color: 'var(--accent)' }}>System Ready</div>
        </div>
      </aside>

      {/* Main Panel Content */}
      <main className="main-content">
        
        {/* Header Status Bar */}
        <header className="header">
          <div className="header-title">
            <h1>Self-Healing Pipeline Monitor</h1>
            <p>E2E Ingestion, Schema Audits & Agent Diagnostics Control Console</p>
          </div>

          <div className={`status-badge ${pipelineStatus === 'healthy' ? 'healthy' : pipelineStatus === 'anomaly' ? 'anomaly' : 'failed'}`}>
            <span className="pulse-dot"></span>
            {pipelineStatus === 'healthy' && 'Pipeline Healthy'}
            {pipelineStatus === 'anomaly' && 'Anomaly Detected'}
            {pipelineStatus === 'failed' && 'Task Failure'}
          </div>
        </header>

        {/* METRICS COUNTER CARDS */}
        <section className="metrics-grid">
          <div className="metric-card">
            <div className="metric-info">
              <h3>Ingestion Success Rate</h3>
              <p className="value">98.2%</p>
            </div>
            <div className="metric-icon-box" style={{ backgroundColor: 'var(--healthy-glow)', color: 'var(--healthy)' }}>
              <CheckCircle2 size={24} />
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-info">
              <h3>Active Quarantines</h3>
              <p className="value">{incidents.filter(i => i.status === 'pending_approval').length}</p>
            </div>
            <div className="metric-icon-box" style={{ backgroundColor: 'var(--failed-glow)', color: 'var(--failed)' }}>
              <ShieldAlert size={24} />
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-info">
              <h3>Resolved Anomaly Loops</h3>
              <p className="value">{remediations.length}</p>
            </div>
            <div className="metric-icon-box" style={{ backgroundColor: 'var(--accent-glow)', color: 'var(--accent)' }}>
              <RefreshCw size={24} />
            </div>
          </div>

          <div className="metric-card">
            <div className="metric-info">
              <h3>BigQuery Dataset Size</h3>
              <p className="value">14.8 MB</p>
            </div>
            <div className="metric-icon-box" style={{ backgroundColor: 'var(--info-glow)', color: 'var(--info)' }}>
              <Database size={24} />
            </div>
          </div>
        </section>

        {activeTab === 'dashboard' && (
          <div className="dashboard-grid">
            
            {/* Left Panel: Statistics and Ingestion Volumes */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
              
              <div className="section-card">
                <div className="section-header">
                  <h2><Activity size={18} /> Ingested Volumes (7-Day History)</h2>
                  <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Fact Table rows / daily batch</span>
                </div>
                
                {/* SVG Column Chart */}
                <div className="chart-container">
                  {volumeData.map((item, index) => (
                    <div className="chart-bar-wrapper" key={index}>
                      <div 
                        className="chart-bar" 
                        style={{ 
                          height: `${Math.min(180, (item.count / 1500) * 160 + 10)}px`,
                          background: item.status === 'anomaly' 
                            ? 'linear-gradient(180deg, var(--failed) 0%, rgba(239, 68, 68, 0.2) 100%)' 
                            : 'linear-gradient(180deg, var(--accent) 0%, rgba(192, 132, 252, 0.2) 100%)'
                        }}
                      >
                        <div className="chart-bar-tooltip">
                          {item.count} rows ({item.status})
                        </div>
                      </div>
                      <span className="chart-label">{item.day}</span>
                    </div>
                  ))}
                </div>
              </div>

              {/* Data quality and schemas validation status table */}
              <div className="section-card">
                <div className="section-header">
                  <h2><FileCode2 size={18} /> BigQuery Database Schema Registry</h2>
                  <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>pipeline_config.yaml verification</span>
                </div>
                
                <div className="table-wrapper">
                  <table>
                    <thead>
                      <tr>
                        <th>Target Table</th>
                        <th>Format</th>
                        <th>Keys Check</th>
                        <th>Freshness SLA</th>
                        <th>Verification Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr>
                        <td><code>dim_customers</code></td>
                        <td>CSV (Staging)</td>
                        <td><code>customer_id</code> (PK)</td>
                        <td>None (Dimension)</td>
                        <td><span style={{ color: 'var(--healthy)', fontWeight: 600 }}>Active / Healthy</span></td>
                      </tr>
                      <tr>
                        <td><code>dim_products</code></td>
                        <td>CSV (Staging)</td>
                        <td><code>product_id</code> (PK)</td>
                        <td>None (Dimension)</td>
                        <td>
                          {activeFault === 'schema_drift' ? (
                            <span style={{ color: 'var(--failed)', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '4px' }}>
                              <AlertCircle size={14} /> Drifted Type
                            </span>
                          ) : (
                            <span style={{ color: 'var(--healthy)', fontWeight: 600 }}>Active / Healthy</span>
                          )}
                        </td>
                      </tr>
                      <tr>
                        <td><code>fct_orders</code></td>
                        <td>CSV Batch (Staged)</td>
                        <td><code>customer_id</code> (FK)</td>
                        <td>Max 26 hours</td>
                        <td>
                          {activeFault === 'null_spike' ? (
                            <span style={{ color: 'var(--failed)', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '4px' }}>
                              <AlertCircle size={14} /> Null Spike Anomaly
                            </span>
                          ) : pipelineStatus === 'anomaly' ? (
                            <span style={{ color: 'var(--warning)', fontWeight: 600, display: 'flex', alignItems: 'center', gap: '4px' }}>
                              <AlertCircle size={14} /> Spike warning
                            </span>
                          ) : (
                            <span style={{ color: 'var(--healthy)', fontWeight: 600 }}>Active / Healthy</span>
                          )}
                        </td>
                      </tr>
                      <tr>
                        <td><code>fct_events</code></td>
                        <td>JSONL Stream</td>
                        <td><code>event_id</code> (PK)</td>
                        <td>Max 6 hours</td>
                        <td><span style={{ color: 'var(--healthy)', fontWeight: 600 }}>Active / Healthy</span></td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* Right Panel: Controls & Remediation Log */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
              
              {/* Fault Injection Panel */}
              <div className="section-card">
                <div className="section-header">
                  <h2><Play size={18} /> Fault Injection Simulator</h2>
                </div>
                <p style={{ fontSize: '13px', color: 'var(--text-secondary)', marginBottom: '16px' }}>
                  Inject sample errors into the staged datasets to evaluate the AI Agent's self-healing triggers and diagnostics.
                </p>

                <div className="fault-injector-grid">
                  <button 
                    className={`fault-btn ${activeFault === 'schema_drift' ? 'active' : ''}`}
                    onClick={() => handleInjectFault('schema_drift')}
                    disabled={activeFault !== null}
                  >
                    <span>Inject Schema Drift (products.csv)</span>
                    <ArrowRight size={16} />
                  </button>
                  <button 
                    className={`fault-btn ${activeFault === 'null_spike' ? 'active' : ''}`}
                    onClick={() => handleInjectFault('null_spike')}
                    disabled={activeFault !== null}
                  >
                    <span>Inject Null Spike (orders.csv)</span>
                    <ArrowRight size={16} />
                  </button>
                  <button 
                    className={`fault-btn ${activeFault === 'ref_break' ? 'active' : ''}`}
                    onClick={() => handleInjectFault('ref_break')}
                    disabled={activeFault !== null}
                  >
                    <span>Inject Referential Breakage</span>
                    <ArrowRight size={16} />
                  </button>
                </div>
              </div>

              {/* Remediation Loops History Log */}
              <div className="section-card">
                <div className="section-header">
                  <h2><RefreshCw size={18} /> Remediation Log</h2>
                </div>
                
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                  {remediations.map((rem, idx) => (
                    <div 
                      key={idx} 
                      style={{ 
                        borderLeft: '3px solid var(--healthy)', 
                        paddingLeft: '12px',
                        fontSize: '13px'
                      }}
                    >
                      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                        <span style={{ fontWeight: 600 }}>{rem.fault} Auto-Fix</span>
                        <span style={{ color: 'var(--text-muted)', fontSize: '11px' }}>{rem.duration}</span>
                      </div>
                      <div style={{ color: 'var(--text-secondary)' }}>
                        Target: <code>{rem.target}</code> | Method: {rem.method}
                      </div>
                      <div style={{ color: 'var(--text-muted)', fontSize: '11px', marginTop: '2px' }}>
                        Timestamp: {rem.time}
                      </div>
                    </div>
                  ))}
                </div>
              </div>

            </div>

          </div>
        )}

        {/* Tab 2: Incidents List */}
        {activeTab === 'incidents' && (
          <div className="section-card" style={{ minHeight: '400px' }}>
            <div className="section-header">
              <h2><AlertTriangle size={18} /> Active Quarantines & Escalations</h2>
            </div>
            
            {incidents.filter(i => i.status === 'pending_approval').length === 0 ? (
              <div style={{ 
                display: 'flex', 
                flexDirection: 'column', 
                alignItems: 'center', 
                justifyContent: 'center', 
                padding: '80px 0',
                color: 'var(--text-secondary)'
              }}>
                <CheckCircle2 size={48} style={{ color: 'var(--healthy)', marginBottom: '16px' }} />
                <h3>No Outstanding Incidents</h3>
                <p style={{ fontSize: '13px' }}>The data pipeline is executing cleanly. Check back if a fault occurs.</p>
              </div>
            ) : (
              incidents.filter(i => i.status === 'pending_approval').map((inc, idx) => (
                <div key={idx} className={`incident-card ${inc.severity === 'medium' ? 'warning' : ''}`}>
                  <div className="incident-header">
                    <div className="incident-title">
                      <h4>
                        <AlertTriangle size={16} style={{ color: inc.severity === 'high' ? 'var(--failed)' : 'var(--warning)' }} />
                        {inc.fault_category} detected on task <code>{inc.task_id}</code>
                      </h4>
                      <p>Triggered: {inc.timestamp}</p>
                    </div>
                    <span className={`severity-tag ${inc.severity}`}>
                      {inc.severity} Severity
                    </span>
                  </div>

                  <div className="incident-body">
                    <p style={{ fontWeight: 500, marginBottom: '6px' }}>Diagnostic Evidence:</p>
                    <div className="incident-evidence">{inc.evidence}</div>
                    
                    <p style={{ margin: '12px 0 6px 0' }}><span style={{ fontWeight: 500 }}>Root Cause Analysis:</span> {inc.root_cause}</p>
                    <p><span style={{ fontWeight: 500 }}>Proposed Remediation:</span> <code>{inc.proposed_action}</code></p>
                  </div>

                  <div className="incident-actions">
                    <button 
                      className="btn btn-primary"
                      onClick={() => handleApproveRemediation(inc.id)}
                      disabled={isProcessing === inc.id}
                    >
                      {isProcessing === inc.id ? (
                        <>
                          <RefreshCw size={14} className="animate-spin" />
                          Processing Fix...
                        </>
                      ) : (
                        <>
                          <UserCheck size={14} />
                          Approve & Execute Fix
                        </>
                      )}
                    </button>
                    <button 
                      className="btn btn-secondary"
                      onClick={() => handleDeclineIncident(inc.id)}
                      disabled={isProcessing === inc.id}
                    >
                      <X size={14} />
                      Decline & Dismiss
                    </button>
                  </div>
                </div>
              ))
            )}

            {/* Resolved incidents table */}
            {incidents.filter(i => i.status === 'remediated').length > 0 && (
              <div style={{ marginTop: '40px' }}>
                <h3 style={{ fontSize: '15px', fontWeight: 600, color: 'var(--text-secondary)', marginBottom: '16px' }}>
                  Recently Remediated Incidents
                </h3>
                <div className="table-wrapper">
                  <table>
                    <thead>
                      <tr>
                        <th>ID</th>
                        <th>Incident</th>
                        <th>Failing Task</th>
                        <th>Resolution Type</th>
                        <th>Status</th>
                      </tr>
                    </thead>
                    <tbody>
                      {incidents.filter(i => i.status === 'remediated').map((inc, idx) => (
                        <tr key={idx}>
                          <td><code>{inc.id}</code></td>
                          <td>{inc.fault_category}</td>
                          <td><code>{inc.task_id}</code></td>
                          <td>Approved Auto-Fix</td>
                          <td>
                            <span style={{ color: 'var(--healthy)', fontWeight: 600, display: 'inline-flex', alignItems: 'center', gap: '4px' }}>
                              <Check size={14} /> Remediated
                            </span>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

          </div>
        )}

        {/* Tab 3: Console Terminal Output Logs */}
        {activeTab === 'logs' && (
          <div className="section-card">
            <div className="section-header">
              <h2><Terminal size={18} /> Local Execution Console</h2>
              <span style={{ fontSize: '12px', color: 'var(--text-secondary)' }}>Live system log stream</span>
            </div>

            <div className="console-logs" id="log-terminal">
              {consoleLogs.map((log, idx) => (
                <div className="log-line" key={idx}>
                  <span className="log-time">[{log.time}]</span>
                  <span className={`log-level ${log.level}`}>{log.level.toUpperCase()}</span>
                  <span className="log-text">{log.text}</span>
                </div>
              ))}
            </div>
          </div>
        )}

      </main>
    </div>
  );
}

export default App;
