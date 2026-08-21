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
  X, 
  RefreshCw, 
  UserCheck, 
  Server, 
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

interface NodeDetails {
  source?: string;
  path?: string;
  primaryKey?: string;
  freshnessSla?: string | null;
  schema?: Record<string, string>;
  description?: string;
  rule?: string;
  nullTolerance?: string;
  range?: string;
  referentialChecks?: string;
  targetTable?: string;
  partitionBy?: string;
  clusterBy?: string;
  mode?: string;
  query?: string;
  activeRules?: string;
}

interface PipelineNode {
  id: string;
  title: string;
  subtitle: string;
  type: 'input' | 'ingest' | 'validate' | 'load' | 'check' | 'monitor';
  details: NodeDetails;
}

function App() {
  const [activeTab, setActiveTab] = useState<'logs' | 'remediations' | 'stats'>('logs');
  const [telemetryOpen, setTelemetryOpen] = useState(true);
  const [pipelineStatus, setPipelineStatus] = useState<'healthy' | 'anomaly' | 'failed'>('healthy');
  const [activeFault, setActiveFault] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState<string | null>(null);
  const [activeNode, setActiveNode] = useState<string>('ingest_orders');
  
  // Incidents state
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

  // Past Remediations
  const [remediations, setRemediations] = useState([
    { id: "rem-091", time: "2026-08-20 10:15:00", fault: "Duplicate Ingestion", target: "fct_orders", method: "Idempotent MERGE", duration: "4.2s", status: "success" },
    { id: "rem-085", time: "2026-08-19 14:02:18", fault: "Staleness Warning", target: "fct_events", method: "Sensor Recrawl", duration: "8.5s", status: "success" }
  ]);

  // Console Logs
  const [consoleLogs, setConsoleLogs] = useState<LogLine[]>([
    { time: "17:56:01", level: "info", text: "Airflow LocalExecutor initialized successfully." },
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
    { day: "08-20", count: 1450, status: "anomaly" } 
  ];

  // Pipeline stages definitions
  const pipelineNodes: PipelineNode[] = [
    {
      id: "source_customers",
      title: "customers.csv",
      subtitle: "Dimension Raw Source",
      type: "input",
      details: {
        source: "CSV Upload",
        path: "data/customers.csv",
        primaryKey: "customer_id",
        freshnessSla: "None (Static Load)",
        schema: {
          customer_id: "string",
          name: "string",
          email: "string",
          region: "string",
          signup_date: "date"
        }
      }
    },
    {
      id: "source_products",
      title: "products.csv",
      subtitle: "Dimension Raw Source",
      type: "input",
      details: {
        source: "CSV Upload",
        path: "data/products.csv",
        primaryKey: "product_id",
        freshnessSla: "None (Static Load)",
        schema: {
          product_id: "string",
          name: "string",
          category: "string",
          price: "float"
        }
      }
    },
    {
      id: "ingest_orders",
      title: "Ingest Orders",
      subtitle: "Task: python_callable",
      type: "ingest",
      details: {
        description: "Ingests the daily orders batch file from the source storage directory.",
        path: "data/orders/orders_{date}.csv",
        primaryKey: "order_id",
        freshnessSla: "Max 26 Hours"
      }
    },
    {
      id: "ingest_events",
      title: "Ingest Events",
      subtitle: "Task: python_callable",
      type: "ingest",
      details: {
        description: "Pulls clickstream events logs via mock api stream and stores them locally.",
        path: "data/events/events_{date}.jsonl",
        primaryKey: "event_id",
        freshnessSla: "Max 6 Hours"
      }
    },
    {
      id: "validate_schema",
      title: "Validate Schema",
      subtitle: "Task: python_callable",
      type: "validate",
      details: {
        description: "Compares current staged orders and events schemas against configuration.",
        rule: "Strict type mapping schema audits. Raises error on drift.",
        activeRules: "customers schema, products schema, orders schema, events schema"
      }
    },
    {
      id: "validate_quality",
      title: "Validate Quality",
      subtitle: "Task: python_callable",
      type: "validate",
      details: {
        description: "Validates null rules, row range thresholds, and referential constraints.",
        nullTolerance: "order_total: 2.0%, customer_id: 0.0%",
        range: "orders: 240 - 360 rows/day | events: 900 - 1500 rows/day",
        referentialChecks: "orders.customer_id -> customers.customer_id, events.customer_id -> customers.customer_id"
      }
    },
    {
      id: "load_orders_bq",
      title: "Load Orders to BQ",
      subtitle: "Task: BigQueryInsertJob",
      type: "load",
      details: {
        targetTable: "fct_orders",
        partitionBy: "order_ts (DAY)",
        clusterBy: "customer_id",
        mode: "WRITE_TRUNCATE (idempotency safety check)"
      }
    },
    {
      id: "bq_row_count_check",
      title: "BQ Row Count Check",
      subtitle: "Task: BigQueryCheck",
      type: "check",
      details: {
        description: "Runs assertion query directly in BigQuery to verify daily row range constraints.",
        query: "SELECT COUNT(*) BETWEEN 240 AND 360 FROM `fct_orders` WHERE DATE(order_ts) = '{{ ds }}'"
      }
    },
    {
      id: "agent_monitor",
      title: "Agent Monitor",
      subtitle: "Task: python_callable",
      type: "monitor",
      details: {
        description: "Validates post-load properties and resolves active pipeline incidents.",
        activeRules: "remediation_policy: duplicate_ingestion=auto_fix, volume_anomaly_spike=auto_fix"
      }
    }
  ];

  // Helper to get status of a node
  const getNodeStatus = (nodeId: string): 'healthy' | 'warning' | 'failed' | 'idle' => {
    if (pipelineStatus === 'healthy') {
      // In healthy state, fct_orders volume spike (from starting incidents) is warning
      if (nodeId === 'validate_quality' && incidents.some(i => i.status === 'pending_approval' && i.fault_category.includes("Volume"))) {
        return 'warning';
      }
      return 'healthy';
    }

    if (activeFault === 'schema_drift') {
      if (nodeId === 'validate_schema') return 'failed';
      // Downstream nodes are idle because the run halted
      const downstream = ['validate_quality', 'load_orders_bq', 'bq_row_count_check', 'agent_monitor'];
      if (downstream.includes(nodeId)) return 'idle';
      return 'healthy';
    }

    if (activeFault === 'null_spike') {
      if (nodeId === 'validate_quality') return 'failed';
      const downstream = ['load_orders_bq', 'bq_row_count_check', 'agent_monitor'];
      if (downstream.includes(nodeId)) return 'idle';
      return 'healthy';
    }

    if (activeFault === 'ref_break') {
      if (nodeId === 'validate_quality') return 'failed';
      const downstream = ['load_orders_bq', 'bq_row_count_check', 'agent_monitor'];
      if (downstream.includes(nodeId)) return 'idle';
      return 'healthy';
    }

    // Default fallbacks
    if (nodeId === 'validate_quality' && incidents.some(i => i.status === 'pending_approval')) {
      return 'warning';
    }

    return 'healthy';
  };

  // Auto-scroll logs
  useEffect(() => {
    const el = document.getElementById('log-terminal');
    if (el) el.scrollTop = el.scrollHeight;
  }, [consoleLogs, telemetryOpen]);

  // Push new log entry helper
  const addLog = (level: LogLine['level'], text: string) => {
    const time = new Date().toTimeString().split(' ')[0];
    setConsoleLogs(prev => [...prev, { time, level, text }]);
  };

  // Inject a fault
  const handleInjectFault = (type: string) => {
    if (activeFault) return; 

    setActiveFault(type);
    setPipelineStatus(type === 'schema_drift' || type === 'null_spike' || type === 'ref_break' ? 'failed' : 'anomaly');

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
      setActiveNode("validate_schema");
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
      setActiveNode("validate_quality");
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
      setActiveNode("validate_quality");
      addLog("warn", "Referential breakage check failed! Missing keys in dimension tables.");
      addLog("info", "AI Agent diagnostics: 3 orders missing corresponding customer keys.");
    }

    setIncidents(prev => [newIncident, ...prev]);
    setTelemetryOpen(true);
    setActiveTab('logs');
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

  // Find active node information
  const selectedNodeInfo = pipelineNodes.find(n => n.id === activeNode);
  const activeIncident = incidents.find(i => i.task_id === activeNode && i.status === 'pending_approval');

  return (
    <div className="app-container">
      {/* Qlik-Style Dark Sidebar */}
      <aside className="sidebar">
        <div className="top-part">
          <div className="logo-section">
            <Activity className="logo-icon" size={20} />
            <span className="logo-text">Qlik Flow</span>
          </div>

          <nav className="nav-links">
            <div className="nav-item active">
              <Server size={16} />
              Pipeline Canvas
            </div>
            <div 
              className="nav-item" 
              onClick={() => {
                setTelemetryOpen(true); 
                setActiveTab('stats');
              }}
            >
              <Database size={16} />
              Metrics & Volumes
            </div>
            <div 
              className="nav-item"
              onClick={() => {
                setTelemetryOpen(true);
                setActiveTab('remediations');
              }}
            >
              <RefreshCw size={16} />
              Remediations
            </div>
          </nav>
        </div>

        <div className="sidebar-footer">
          <div>Self-Healing Pipeline</div>
          <div style={{ color: 'var(--accent)', marginTop: '2px', fontWeight: 600 }}>Active Workspace</div>
        </div>
      </aside>

      {/* Main Workspace Frame */}
      <main className="main-content">
        {/* Header bar */}
        <header className="header">
          <div className="header-title">
            <h1>Pipeline Diagnostics Canvas</h1>
            <p>Visual orchestrator & self-healing controller</p>
          </div>

          <div className={`status-badge ${pipelineStatus === 'healthy' ? 'healthy' : pipelineStatus === 'anomaly' ? 'anomaly' : 'failed'}`}>
            <span className="pulse-dot"></span>
            {pipelineStatus === 'healthy' && 'Pipeline Healthy'}
            {pipelineStatus === 'anomaly' && 'Anomaly Flagged'}
            {pipelineStatus === 'failed' && 'Task Failure'}
          </div>
        </header>

        {/* Fault Injection Control Bar */}
        <div className="workspace-toolbar">
          <div className="toolbar-section">
            <span className="toolbar-label">Simulation Engine</span>
            <div className="toolbar-divider"></div>
            <button 
              className={`fault-pill ${activeFault === 'schema_drift' ? 'active' : ''}`}
              onClick={() => handleInjectFault('schema_drift')}
              disabled={activeFault !== null}
            >
              <Play size={12} />
              Schema Drift
            </button>
            <button 
              className={`fault-pill ${activeFault === 'null_spike' ? 'active' : ''}`}
              onClick={() => handleInjectFault('null_spike')}
              disabled={activeFault !== null}
            >
              <Play size={12} />
              Null Spike
            </button>
            <button 
              className={`fault-pill ${activeFault === 'ref_break' ? 'active' : ''}`}
              onClick={() => handleInjectFault('ref_break')}
              disabled={activeFault !== null}
            >
              <Play size={12} />
              Referential Break
            </button>
          </div>

          <div className="toolbar-section">
            <button 
              className="fault-pill" 
              onClick={() => setTelemetryOpen(!telemetryOpen)}
            >
              <Terminal size={12} />
              {telemetryOpen ? 'Hide Logs' : 'Show Logs'}
            </button>
          </div>
        </div>

        {/* Workspace Canvas (Dotted Grid) */}
        <div className="canvas-workspace">
          <div className="pipeline-flow">
            
            {/* Column 1: Sources */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Data Sources</div>
              
              <div 
                className={`node-card info ${activeNode === 'source_customers' ? 'selected' : ''}`}
                onClick={() => setActiveNode('source_customers')}
              >
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><Database size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">customers.csv</h3>
                <p className="node-subtitle">Staged Dimensions</p>
              </div>

              <div 
                className={`node-card info ${activeNode === 'source_products' ? 'selected' : ''}`}
                onClick={() => setActiveNode('source_products')}
              >
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><Database size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">products.csv</h3>
                <p className="node-subtitle">Staged Dimensions</p>
              </div>
            </div>

            {/* Column 2: Ingestion */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Ingestion</div>

              <div 
                className={`node-card ${getNodeStatus('ingest_orders')} ${activeNode === 'ingest_orders' ? 'selected' : ''}`}
                onClick={() => setActiveNode('ingest_orders')}
              >
                <div className="node-connector input"></div>
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><Server size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Ingest Orders</h3>
                <p className="node-subtitle">orders_{`{date}`}.csv</p>
              </div>

              <div 
                className={`node-card ${getNodeStatus('ingest_events')} ${activeNode === 'ingest_events' ? 'selected' : ''}`}
                onClick={() => setActiveNode('ingest_events')}
              >
                <div className="node-connector input"></div>
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><Server size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Ingest Events</h3>
                <p className="node-subtitle">events_{`{date}`}.jsonl</p>
              </div>
            </div>

            {/* Column 3: Quality Audits */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Quality Audits</div>

              <div 
                className={`node-card ${getNodeStatus('validate_schema')} ${activeNode === 'validate_schema' ? 'selected' : ''}`}
                onClick={() => setActiveNode('validate_schema')}
              >
                <div className="node-connector input"></div>
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><FileCode2 size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Validate Schema</h3>
                <p className="node-subtitle">Structure Checks</p>
              </div>

              <div 
                className={`node-card ${getNodeStatus('validate_quality')} ${activeNode === 'validate_quality' ? 'selected' : ''}`}
                onClick={() => setActiveNode('validate_quality')}
              >
                <div className="node-connector input"></div>
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><AlertTriangle size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Validate Quality</h3>
                <p className="node-subtitle">Values & Null Audits</p>
              </div>
            </div>

            {/* Column 4: Warehouse Load */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Storage</div>

              <div 
                className={`node-card ${getNodeStatus('load_orders_bq')} ${activeNode === 'load_orders_bq' ? 'selected' : ''}`}
                onClick={() => setActiveNode('load_orders_bq')}
              >
                <div className="node-connector input"></div>
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><Database size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Load Orders BQ</h3>
                <p className="node-subtitle">BigQuery Load</p>
              </div>
            </div>

            {/* Column 5: Post-Load check */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Assertions</div>

              <div 
                className={`node-card ${getNodeStatus('bq_row_count_check')} ${activeNode === 'bq_row_count_check' ? 'selected' : ''}`}
                onClick={() => setActiveNode('bq_row_count_check')}
              >
                <div className="node-connector input"></div>
                <div className="node-connector output"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><CheckCircle2 size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Row Count Check</h3>
                <p className="node-subtitle">Declarative SQL</p>
              </div>
            </div>

            {/* Column 6: Diagnostics */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Agent</div>

              <div 
                className={`node-card ${getNodeStatus('agent_monitor')} ${activeNode === 'agent_monitor' ? 'selected' : ''}`}
                onClick={() => setActiveNode('agent_monitor')}
              >
                <div className="node-connector input"></div>
                <div className="node-header">
                  <div className="node-icon-wrapper"><Activity size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Agent Monitor</h3>
                <p className="node-subtitle">Remediation Loop</p>
              </div>
            </div>

          </div>
        </div>

        {/* Right side slide-over Configuration Inspector */}
        {selectedNodeInfo && (
          <aside className={`inspector-panel ${selectedNodeInfo ? 'open' : ''}`}>
            <div className="inspector-header">
              <div className="inspector-title-area">
                <h2>{selectedNodeInfo.title}</h2>
                <p>{selectedNodeInfo.subtitle}</p>
              </div>
              <button className="inspector-close-btn" onClick={() => setActiveNode('')}>
                <X size={16} />
              </button>
            </div>

            <div className="inspector-content">
              {/* Active Incident Warning box */}
              {activeIncident ? (
                <div className={`inspector-incident-card ${activeIncident.severity === 'medium' ? 'warning' : ''}`}>
                  <div className="incident-badge-row">
                    <span style={{ fontWeight: 700, fontSize: '12px', display: 'flex', alignItems: 'center', gap: '6px', color: activeIncident.severity === 'high' ? '#ef4444' : '#d97706' }}>
                      <AlertTriangle size={14} />
                      Incident {activeIncident.id}
                    </span>
                    <span className={`severity-pill ${activeIncident.severity}`}>
                      {activeIncident.severity}
                    </span>
                  </div>

                  <p style={{ margin: '0 0 6px 0', fontSize: '11px', fontWeight: 600 }}>Diagnostic Evidence:</p>
                  <div className="incident-evidence-box">
                    {activeIncident.evidence}
                  </div>

                  <div style={{ fontSize: '11px', lineHeight: 1.4, color: '#334155' }}>
                    <p style={{ margin: '4px 0' }}><span style={{ fontWeight: 600 }}>Root Cause:</span> {activeIncident.root_cause}</p>
                    <p style={{ margin: '4px 0' }}><span style={{ fontWeight: 600 }}>Proposed Fix:</span> {activeIncident.proposed_action}</p>
                  </div>

                  <div className="action-buttons-group">
                    <button 
                      className="btn btn-action-primary"
                      onClick={() => handleApproveRemediation(activeIncident.id)}
                      disabled={isProcessing === activeIncident.id}
                    >
                      {isProcessing === activeIncident.id ? (
                        <>
                          <RefreshCw size={12} className="animate-spin" />
                          Executing...
                        </>
                      ) : (
                        <>
                          <UserCheck size={12} />
                          Approve Fix
                        </>
                      )}
                    </button>
                    <button 
                      className="btn btn-action-secondary"
                      onClick={() => handleDeclineIncident(activeIncident.id)}
                      disabled={isProcessing === activeIncident.id}
                    >
                      Dismiss
                    </button>
                  </div>
                </div>
              ) : (
                getNodeStatus(selectedNodeInfo.id) === 'failed' && (
                  <div className="inspector-incident-card">
                    <div style={{ fontSize: '12px', color: '#b91c1c', display: 'flex', alignItems: 'center', gap: '6px', fontWeight: 600 }}>
                      <AlertCircle size={14} />
                      Execution Halted
                    </div>
                    <p style={{ fontSize: '11px', color: '#7f1d1d', margin: '4px 0 0 0' }}>This task failed. Check parent nodes or logs.</p>
                  </div>
                )
              )}

              {/* Node specifications */}
              <div className="inspector-section">
                <h3 className="inspector-section-title">Configuration</h3>
                
                <div className="property-grid">
                  {selectedNodeInfo.details.source && (
                    <div className="property-row">
                      <span className="property-label">Source System</span>
                      <span className="property-value">{selectedNodeInfo.details.source}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.path && (
                    <div className="property-row">
                      <span className="property-label">File Pattern</span>
                      <span className="property-value mono">{selectedNodeInfo.details.path}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.primaryKey && (
                    <div className="property-row">
                      <span className="property-label">Primary Key</span>
                      <span className="property-value mono">{selectedNodeInfo.details.primaryKey}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.freshnessSla && (
                    <div className="property-row">
                      <span className="property-label">Freshness SLA</span>
                      <span className="property-value">{selectedNodeInfo.details.freshnessSla}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.targetTable && (
                    <div className="property-row">
                      <span className="property-label">Target BQ Table</span>
                      <span className="property-value mono">{selectedNodeInfo.details.targetTable}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.partitionBy && (
                    <div className="property-row">
                      <span className="property-label">Partition Field</span>
                      <span className="property-value mono">{selectedNodeInfo.details.partitionBy}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.clusterBy && (
                    <div className="property-row">
                      <span className="property-label">Cluster Field</span>
                      <span className="property-value mono">{selectedNodeInfo.details.clusterBy}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.mode && (
                    <div className="property-row">
                      <span className="property-label">Write Mode</span>
                      <span className="property-value">{selectedNodeInfo.details.mode}</span>
                    </div>
                  )}
                  {selectedNodeInfo.details.description && (
                    <div className="property-row">
                      <span className="property-label">Goal</span>
                      <span className="property-value">{selectedNodeInfo.details.description}</span>
                    </div>
                  )}
                </div>
              </div>

              {/* Quality rules */}
              {(selectedNodeInfo.details.rule || selectedNodeInfo.details.nullTolerance || selectedNodeInfo.details.range || selectedNodeInfo.details.query) && (
                <div className="inspector-section">
                  <h3 className="inspector-section-title">Validation Rules</h3>
                  <div className="property-grid">
                    {selectedNodeInfo.details.rule && (
                      <div className="property-row">
                        <span className="property-label">Audit Engine</span>
                        <span className="property-value">{selectedNodeInfo.details.rule}</span>
                      </div>
                    )}
                    {selectedNodeInfo.details.nullTolerance && (
                      <div className="property-row">
                        <span className="property-label">Null Limit</span>
                        <span className="property-value mono">{selectedNodeInfo.details.nullTolerance}</span>
                      </div>
                    )}
                    {selectedNodeInfo.details.range && (
                      <div className="property-row">
                        <span className="property-label">Expected Row Bounds</span>
                        <span className="property-value">{selectedNodeInfo.details.range}</span>
                      </div>
                    )}
                    {selectedNodeInfo.details.referentialChecks && (
                      <div className="property-row">
                        <span className="property-label">Foreign Constraints</span>
                        <span className="property-value mono">{selectedNodeInfo.details.referentialChecks}</span>
                      </div>
                    )}
                    {selectedNodeInfo.details.query && (
                      <div className="property-row" style={{ flexDirection: 'column', gap: '6px' }}>
                        <span className="property-label">Check Operator SQL</span>
                        <pre style={{ margin: 0, padding: '8px', backgroundColor: 'var(--bg-canvas)', border: '1px solid var(--border-color)', borderRadius: '4px', fontFamily: 'var(--mono)', fontSize: '10px', overflowX: 'auto', whiteSpace: 'pre-wrap' }}>
                          {selectedNodeInfo.details.query}
                        </pre>
                      </div>
                    )}
                  </div>
                </div>
              )}

              {/* Node Schema Registry list */}
              {selectedNodeInfo.details.schema && (
                <div className="inspector-section">
                  <h3 className="inspector-section-title">Schema Fields</h3>
                  <div className="schema-list">
                    {Object.entries(selectedNodeInfo.details.schema).map(([field, type]) => (
                      <div className="schema-item" key={field}>
                        <span className="schema-field">{field}</span>
                        <span className="schema-type">{type}</span>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          </aside>
        )}

        {/* Collapsible Telemetry / Drawer */}
        <section className={`telemetry-drawer ${telemetryOpen ? 'open' : ''}`}>
          <div className="telemetry-header">
            <div className="telemetry-tabs">
              <button 
                className={`telemetry-tab-btn ${activeTab === 'logs' ? 'active' : ''}`}
                onClick={() => setActiveTab('logs')}
              >
                Local Logs
              </button>
              <button 
                className={`telemetry-tab-btn ${activeTab === 'remediations' ? 'active' : ''}`}
                onClick={() => setActiveTab('remediations')}
              >
                Remediation Audit
              </button>
              <button 
                className={`telemetry-tab-btn ${activeTab === 'stats' ? 'active' : ''}`}
                onClick={() => setActiveTab('stats')}
              >
                Telemetry Statistics
              </button>
            </div>
            <button className="telemetry-close-btn" onClick={() => setTelemetryOpen(false)}>
              <X size={14} />
            </button>
          </div>

          <div className="telemetry-content">
            
            {/* Terminal logs list */}
            {activeTab === 'logs' && (
              <div className="terminal-console" id="log-terminal">
                {consoleLogs.map((log, idx) => (
                  <div className="terminal-line" key={idx}>
                    <span className="terminal-time">[{log.time}]</span>
                    <span className={`terminal-level ${log.level}`}>{log.level.toUpperCase()}</span>
                    <span>{log.text}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Remediation Loops list */}
            {activeTab === 'remediations' && (
              <div className="remediation-history-list">
                {remediations.map((rem, idx) => (
                  <div className="rem-history-item" key={idx}>
                    <div className="rem-history-left">
                      <span className="rem-history-title">{rem.fault} Auto-Remediation</span>
                      <span className="rem-history-desc">Target Table: <code>{rem.target}</code> | Method: {rem.method}</span>
                    </div>
                    <div className="rem-history-right">
                      <span className="rem-history-time">{rem.time}</span>
                      <div>
                        <span className="rem-history-badge">PASSED ({rem.duration})</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}

            {/* Pipeline telemetry metrics */}
            {activeTab === 'stats' && (
              <div className="stats-grid">
                
                <div className="stat-item">
                  <div className="stat-info">
                    <h4>Success Rate</h4>
                    <p className="stat-val">98.2%</p>
                  </div>
                  <div className="stat-icon"><CheckCircle2 size={16} /></div>
                </div>

                <div className="stat-item">
                  <div className="stat-info">
                    <h4>Quarantines</h4>
                    <p className="stat-val">{incidents.filter(i => i.status === 'pending_approval').length}</p>
                  </div>
                  <div className="stat-icon" style={{ color: '#ef4444', backgroundColor: 'rgba(239, 68, 68, 0.1)' }}><ShieldAlert size={16} /></div>
                </div>

                <div className="stat-item">
                  <div className="stat-info">
                    <h4>Analytic Storage</h4>
                    <p className="stat-val">14.8 MB</p>
                  </div>
                  <div className="stat-icon" style={{ color: '#0ea5e9', backgroundColor: 'rgba(14, 165, 233, 0.1)' }}><Database size={16} /></div>
                </div>

                {/* SVG Mini bar chart */}
                <div className="stat-item" style={{ padding: '8px 12px' }}>
                  <div className="stat-info" style={{ marginRight: '12px' }}>
                    <h4>Row Throughput</h4>
                    <p className="stat-val" style={{ fontSize: '13px' }}>7-Day History</p>
                  </div>
                  <div className="drawer-chart-container">
                    {volumeData.map((item, idx) => (
                      <div className="drawer-chart-bar-wrapper" key={idx}>
                        <div 
                          className={`drawer-chart-bar ${item.status === 'anomaly' ? 'anomaly' : ''}`}
                          style={{ height: `${Math.min(90, (item.count / 1500) * 80 + 5)}px` }}
                        ></div>
                        <span className="drawer-chart-label">{item.day.split('-')[1]}</span>
                      </div>
                    ))}
                  </div>
                </div>

              </div>
            )}

          </div>
        </section>

      </main>
    </div>
  );
}

export default App;
