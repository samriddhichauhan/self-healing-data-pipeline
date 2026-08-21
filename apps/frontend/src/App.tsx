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
  AlertCircle,
  Sun,
  Moon
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
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [activeTab, setActiveTab] = useState<'logs' | 'remediations' | 'stats'>('logs');
  const [telemetryOpen, setTelemetryOpen] = useState(true);
  const [pipelineStatus, setPipelineStatus] = useState<'healthy' | 'anomaly' | 'failed'>('healthy');
  const [activeFault, setActiveFault] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState<string | null>(null);
  const [activeNode, setActiveNode] = useState<string>('ingest_orders');
  
  // Pipeline node running states
  const [nodeStatuses, setNodeStatuses] = useState<Record<string, 'healthy' | 'warning' | 'failed' | 'idle' | 'running'>>({});
  const [isSimulationActive, setIsSimulationActive] = useState(false);
  const [connections, setConnections] = useState<Array<{ from: string, to: string, path: string, status: string }>>([]);

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
      title: "Load Orders BQ",
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

  // Sync node statuses dynamically when simulation is inactive
  useEffect(() => {
    if (isSimulationActive) return;

    const getNodeStatus = (nodeId: string): 'healthy' | 'warning' | 'failed' | 'idle' | 'running' => {
      if (pipelineStatus === 'healthy') {
        if (nodeId === 'validate_quality' && incidents.some(i => i.status === 'pending_approval' && i.fault_category.includes("Volume"))) {
          return 'warning';
        }
        return 'healthy';
      }

      if (activeFault === 'schema_drift') {
        if (nodeId === 'validate_schema') return 'failed';
        const downstream = ['validate_quality', 'load_orders_bq', 'bq_row_count_check', 'agent_monitor'];
        if (downstream.includes(nodeId)) return 'idle';
        return 'healthy';
      }

      if (activeFault === 'null_spike' || activeFault === 'ref_break') {
        if (nodeId === 'validate_quality') return 'failed';
        const downstream = ['load_orders_bq', 'bq_row_count_check', 'agent_monitor'];
        if (downstream.includes(nodeId)) return 'idle';
        return 'healthy';
      }

      if (nodeId === 'validate_quality' && incidents.some(i => i.status === 'pending_approval')) {
        return 'warning';
      }

      return 'healthy';
    };

    const initial: Record<string, 'healthy' | 'warning' | 'failed' | 'idle' | 'running'> = {
      source_customers: 'healthy',
      source_products: 'healthy',
      ingest_orders: getNodeStatus('ingest_orders'),
      ingest_events: getNodeStatus('ingest_events'),
      validate_schema: getNodeStatus('validate_schema'),
      validate_quality: getNodeStatus('validate_quality'),
      load_orders_bq: getNodeStatus('load_orders_bq'),
      bq_row_count_check: getNodeStatus('bq_row_count_check'),
      agent_monitor: getNodeStatus('agent_monitor'),
    };
    setNodeStatuses(initial);
  }, [activeFault, pipelineStatus, incidents, isSimulationActive]);

  // Recalculate dynamic ports connections coordinates
  const updateConnections = () => {
    const flowElement = document.querySelector('.pipeline-flow');
    if (!flowElement) return;
    const flowRect = flowElement.getBoundingClientRect();
    
    const newConnections: Array<{ from: string, to: string, path: string, status: string }> = [];
    const linkPairs = [
      { from: 'source_customers', to: 'ingest_orders' },
      { from: 'source_products', to: 'ingest_orders' },
      { from: 'ingest_orders', to: 'validate_schema' },
      { from: 'ingest_events', to: 'validate_schema' },
      { from: 'validate_schema', to: 'validate_quality' },
      { from: 'validate_quality', to: 'load_orders_bq' },
      { from: 'load_orders_bq', to: 'bq_row_count_check' },
      { from: 'bq_row_count_check', to: 'agent_monitor' }
    ];
    
    linkPairs.forEach(({ from, to }) => {
      const fromEl = document.querySelector(`[data-node-id="${from}"]`);
      const toEl = document.querySelector(`[data-node-id="${to}"]`);
      if (fromEl && toEl) {
        const fromRect = fromEl.getBoundingClientRect();
        const toRect = toEl.getBoundingClientRect();
        
        const x1 = fromRect.right - flowRect.left;
        const y1 = fromRect.top + fromRect.height / 2 - flowRect.top;
        
        const x2 = toRect.left - flowRect.left;
        const y2 = toRect.top + toRect.height / 2 - flowRect.top;
        
        const dx = Math.abs(x2 - x1) * 0.45;
        const path = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
        
        let status = 'idle';
        const fromStatus = nodeStatuses[from] || 'idle';
        const toStatus = nodeStatuses[to] || 'idle';
        
        if (fromStatus === 'running' || toStatus === 'running') {
          status = 'running';
        } else if (fromStatus === 'failed' || toStatus === 'failed') {
          status = 'failed';
        } else if (fromStatus === 'healthy' && toStatus === 'healthy') {
          status = 'active';
        }
        
        newConnections.push({ from, to, path, status });
      }
    });
    setConnections(newConnections);
  };

  // Re-run connection path checks on UI shift triggers
  useEffect(() => {
    const timer = setTimeout(() => {
      updateConnections();
    }, 120);
    
    window.addEventListener('resize', updateConnections);
    return () => {
      clearTimeout(timer);
      window.removeEventListener('resize', updateConnections);
    };
  }, [activeNode, pipelineStatus, activeFault, nodeStatuses, telemetryOpen, theme]);

  // Push new log entry
  const addLog = (level: LogLine['level'], text: string) => {
    const time = new Date().toTimeString().split(' ')[0];
    setConsoleLogs(prev => [...prev, { time, level, text }]);
  };

  // Trigger step-by-step simulation run
  const runPipelineSimulation = () => {
    if (isSimulationActive) return;
    setIsSimulationActive(true);
    setPipelineStatus('healthy'); 
    
    // Clear and reset values
    const resetStates: Record<string, 'healthy' | 'warning' | 'failed' | 'idle' | 'running'> = {
      source_customers: 'healthy',
      source_products: 'healthy',
      ingest_orders: 'idle',
      ingest_events: 'idle',
      validate_schema: 'idle',
      validate_quality: 'idle',
      load_orders_bq: 'idle',
      bq_row_count_check: 'idle',
      agent_monitor: 'idle'
    };
    setNodeStatuses(resetStates);
    setConsoleLogs([]);
    
    addLog("info", "Starting E2E self-healing data pipeline run...");
    addLog("info", "Initializing execution environment & credentials check...");
    
    // Step 1: Ingest
    setTimeout(() => {
      setNodeStatuses(prev => ({ ...prev, ingest_orders: 'running', ingest_events: 'running' }));
      addLog("info", "Executing task: ingest_orders (loading CSV orders stream)...");
      addLog("info", "Executing task: ingest_events (fetching clickstream mock JSONL)...");
      
      setTimeout(() => {
        setNodeStatuses(prev => ({ ...prev, ingest_orders: 'healthy', ingest_events: 'healthy', validate_schema: 'running' }));
        addLog("success", "Ingest Orders: successfully loaded 300 order rows.");
        addLog("success", "Ingest Events: successfully ingested 1,200 events stream.");
        addLog("info", "Executing task: validate_schema (strict layout check)...");
        
        setTimeout(() => {
          if (activeFault === 'schema_drift') {
            setNodeStatuses(prev => ({ ...prev, validate_schema: 'failed' }));
            setPipelineStatus('failed');
            addLog("error", "Task validate_schema failed! Schema drift detected in products.csv.");
            addLog("warn", "Field 'price' expected type FLOAT, got STRING (e.g. '$14.99').");
            addLog("info", "Triggering AI Diagnostics Agent callback...");
            
            const newInc: Incident = {
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
            setIncidents(prev => [newInc, ...prev.filter(i => i.task_id !== 'validate_schema')]);
            setActiveNode("validate_schema");
            setIsSimulationActive(false);
            return;
          }
          
          setNodeStatuses(prev => ({ ...prev, validate_schema: 'healthy', validate_quality: 'running' }));
          addLog("success", "Schema validation PASSED. All columns match definitions.");
          addLog("info", "Executing task: validate_quality (value constraints check)...");
          
          setTimeout(() => {
            if (activeFault === 'null_spike') {
              setNodeStatuses(prev => ({ ...prev, validate_quality: 'failed' }));
              setPipelineStatus('failed');
              addLog("error", "Task validate_quality failed! Null spike detected in customer_id.");
              addLog("warn", "Field 'customer_id' contains 14.5% null values (threshold: 0.0%).");
              addLog("info", "Triggering AI Diagnostics Agent callback...");
              
              const newInc: Incident = {
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
              setIncidents(prev => [newInc, ...prev.filter(i => i.task_id !== 'validate_quality')]);
              setActiveNode("validate_quality");
              setIsSimulationActive(false);
              return;
            }
            
            if (activeFault === 'ref_break') {
              setNodeStatuses(prev => ({ ...prev, validate_quality: 'failed' }));
              setPipelineStatus('failed');
              addLog("error", "Task validate_quality failed! Referential key constraint violation.");
              addLog("warn", "Staged orders reference customer_id 'C-9988' which is missing in dim_customers.");
              addLog("info", "Triggering AI Diagnostics Agent callback...");
              
              const newInc: Incident = {
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
              setIncidents(prev => [newInc, ...prev.filter(i => i.task_id !== 'validate_quality')]);
              setActiveNode("validate_quality");
              setIsSimulationActive(false);
              return;
            }
            
            setNodeStatuses(prev => ({ ...prev, validate_quality: 'healthy', load_orders_bq: 'running' }));
            addLog("success", "Data quality checks PASSED. Row bounds and foreign keys verified.");
            addLog("info", "Executing task: load_orders_to_bq (loading structured data to BQ)...");
            
            setTimeout(() => {
              setNodeStatuses(prev => ({ ...prev, load_orders_bq: 'healthy', bq_row_count_check: 'running' }));
              addLog("success", "BigQuery Load completed: 300 rows successfully loaded into fct_orders.");
              addLog("info", "Executing task: check_orders_row_count (BigQuery row assert check)...");
              
              setTimeout(() => {
                setNodeStatuses(prev => ({ ...prev, bq_row_count_check: 'healthy', agent_monitor: 'running' }));
                addLog("success", "BigQuery check PASSED: Row count falls within expected bounds.");
                addLog("info", "Executing task: agent_monitor (post-load pipeline telemetry review)...");
                
                setTimeout(() => {
                  setNodeStatuses(prev => ({ ...prev, agent_monitor: 'healthy' }));
                  setPipelineStatus('healthy');
                  addLog("success", "E2E Pipeline run completed successfully!");
                  addLog("success", "All steps are healthy. Pipeline execution status: GREEN.");
                  setIsSimulationActive(false);
                }, 1200);
              }, 1200);
            }, 1200);
          }, 1200);
        }, 1200);
      }, 1200);
    }, 1500);
  };

  // Run single node task execution in inspector
  const handleRunSingleStep = (nodeId: string) => {
    setNodeStatuses(prev => ({ ...prev, [nodeId]: 'running' }));
    addLog("info", `Manually triggering task run: ${nodeId}...`);
    
    setTimeout(() => {
      const finalStatus = activeFault && nodeId === (activeFault === 'schema_drift' ? 'validate_schema' : 'validate_quality') ? 'failed' : 'healthy';
      setNodeStatuses(prev => ({ ...prev, [nodeId]: finalStatus }));
      
      if (finalStatus === 'failed') {
        addLog("error", `Task ${nodeId} execution failed!`);
        setPipelineStatus('failed');
      } else {
        addLog("success", `Task ${nodeId} executed successfully.`);
      }
    }, 1200);
  };

  // Inject a fault manually
  const handleInjectFault = (type: string) => {
    if (activeFault || isSimulationActive) return; 

    setActiveFault(type);
    setPipelineStatus('failed');

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
      setNodeStatuses(prev => ({ ...prev, validate_schema: 'failed', validate_quality: 'idle', load_orders_bq: 'idle', bq_row_count_check: 'idle', agent_monitor: 'idle' }));
      addLog("error", "Task validate_schema failed! Schema drift detected in products.csv.");
      addLog("warn", "Field 'price' expected type FLOAT, got STRING (e.g. '$14.99').");
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
      setNodeStatuses(prev => ({ ...prev, validate_quality: 'failed', load_orders_bq: 'idle', bq_row_count_check: 'idle', agent_monitor: 'idle' }));
      addLog("error", "Task validate_quality failed! Null spike detected in customer_id.");
      addLog("warn", "Field 'customer_id' contains 14.5% null values.");
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
      setNodeStatuses(prev => ({ ...prev, validate_quality: 'failed', load_orders_bq: 'idle', bq_row_count_check: 'idle', agent_monitor: 'idle' }));
      addLog("error", "Task validate_quality failed! Referential key constraint violation.");
      addLog("warn", "Missing reference: C-9988 not found in dim_customers.");
    }

    setIncidents(prev => [newIncident, ...prev.filter(i => i.task_id !== newIncident.task_id)]);
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

  // Format visual badges for schema data types
  const renderDataTypeBadge = (type: string) => {
    return <span className="schema-type">{type.toUpperCase()}</span>;
  };

  // Dynamic SVG Area Chart render
  const renderStatsChart = () => {
    const eventsData = [1210, 1195, 1250, 1180, 1200, 1220, 1205];
    const maxVal = 1500;
    
    // Generate points for Orders
    const pointsOrders = volumeData.map((item, idx) => {
      const x = idx * (360 / 6);
      const y = 80 - (item.count / maxVal) * 70;
      return { x, y };
    });
    const lineOrdersD = pointsOrders.map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    const areaOrdersD = `${lineOrdersD} L ${pointsOrders[pointsOrders.length - 1].x} 90 L ${pointsOrders[0].x} 90 Z`;
    
    // Generate points for Events
    const pointsEvents = eventsData.map((val, idx) => {
      const x = idx * (360 / 6);
      const y = 80 - (val / maxVal) * 70;
      return { x, y };
    });
    const lineEventsD = pointsEvents.map((p, idx) => `${idx === 0 ? 'M' : 'L'} ${p.x} ${p.y}`).join(' ');
    
    return (
      <div className="drawer-chart-container">
        <svg className="chart-svg-layer" viewBox="0 0 360 90" preserveAspectRatio="none">
          <defs>
            <linearGradient id="area-grad" x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stopColor="var(--accent)" stopOpacity="0.3" />
              <stop offset="100%" stopColor="var(--accent)" stopOpacity="0.0" />
            </linearGradient>
          </defs>
          
          {/* Grid lines */}
          <line x1="0" y1="10" x2="360" y2="10" stroke="rgba(255,255,255,0.03)" strokeWidth="1" />
          <line x1="0" y1="45" x2="360" y2="45" stroke="rgba(255,255,255,0.03)" strokeWidth="1" />
          <line x1="0" y1="80" x2="360" y2="80" stroke="rgba(255,255,255,0.03)" strokeWidth="1" />
          
          {/* Area fill for Orders */}
          <path d={areaOrdersD} className="chart-gradient-path" />
          
          {/* Line for Orders */}
          <path d={lineOrdersD} className="chart-line-path" />
          
          {/* Line for Events */}
          <path d={lineEventsD} className="chart-events-line-path" />
          
          {/* Data points dots */}
          {pointsOrders.map((p, idx) => (
            <circle key={`ord-${idx}`} cx={p.x} cy={p.y} r="2.5" fill="var(--accent)" stroke="#ffffff" strokeWidth="1" />
          ))}
        </svg>
      </div>
    );
  };

  const selectedNodeInfo = pipelineNodes.find(n => n.id === activeNode);
  const activeIncident = incidents.find(i => i.task_id === activeNode && i.status === 'pending_approval');

  return (
    <div className={`app-container ${theme}-theme`}>
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

        <div>
          {/* Theme Toggle Button */}
          <div className="sidebar-controls">
            <button 
              className="theme-toggle-btn"
              onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
            >
              {theme === 'light' ? (
                <>
                  <Moon size={13} />
                  <span>Switch to Dark Mode</span>
                </>
              ) : (
                <>
                  <Sun size={13} />
                  <span>Switch to Light Mode</span>
                </>
              )}
            </button>
          </div>
          
          <div className="sidebar-footer">
            <div>Self-Healing Pipeline</div>
            <div style={{ color: 'var(--accent)', marginTop: '2px', fontWeight: 600 }}>Active Workspace</div>
          </div>
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
            <span className="toolbar-label">Simulation Control</span>
            <button 
              className="fault-pill btn-control-play"
              onClick={runPipelineSimulation}
              disabled={isSimulationActive}
            >
              {isSimulationActive ? (
                <>
                  <RefreshCw size={12} className="animate-spin" />
                  <span>Simulating...</span>
                </>
              ) : (
                <>
                  <Play size={12} fill="currentColor" />
                  <span>Run Pipeline</span>
                </>
              )}
            </button>
            
            <div className="toolbar-divider"></div>
            <span className="toolbar-label">Faults Injection</span>
            
            <button 
              className={`fault-pill ${activeFault === 'schema_drift' ? 'active' : ''}`}
              onClick={() => handleInjectFault('schema_drift')}
              disabled={activeFault !== null || isSimulationActive}
            >
              Schema Drift
            </button>
            <button 
              className={`fault-pill ${activeFault === 'null_spike' ? 'active' : ''}`}
              onClick={() => handleInjectFault('null_spike')}
              disabled={activeFault !== null || isSimulationActive}
            >
              Null Spike
            </button>
            <button 
              className={`fault-pill ${activeFault === 'ref_break' ? 'active' : ''}`}
              onClick={() => handleInjectFault('ref_break')}
              disabled={activeFault !== null || isSimulationActive}
            >
              Referential Break
            </button>
          </div>

          <div className="toolbar-section">
            <button 
              className="fault-pill" 
              onClick={() => setTelemetryOpen(!telemetryOpen)}
            >
              <Terminal size={12} />
              {telemetryOpen ? 'Hide Drawer' : 'Show Drawer'}
            </button>
          </div>
        </div>

        {/* Workspace Canvas (Dotted Grid with SVG Connection Overlay) */}
        <div className="canvas-workspace">
          <div className="pipeline-flow">
            {/* Dynamic Bezier SVG Connection Layer */}
            <svg className="pipeline-svg-connections">
              {connections.map((conn, idx) => (
                <path 
                  key={idx}
                  d={conn.path}
                  className={`pipeline-connection-path ${conn.status}`}
                />
              ))}
            </svg>
            
            {/* Column 1: Sources */}
            <div className="flow-column">
              <div className="flow-link-label" style={{ top: '-14px' }}>Data Sources</div>
              
              <div 
                data-node-id="source_customers"
                className={`node-card info ${activeNode === 'source_customers' ? 'selected' : ''}`}
                onClick={() => setActiveNode('source_customers')}
              >
                <div className="node-header">
                  <div className="node-icon-wrapper"><Database size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">customers.csv</h3>
                <p className="node-subtitle">Staged Dimensions</p>
              </div>

              <div 
                data-node-id="source_products"
                className={`node-card info ${activeNode === 'source_products' ? 'selected' : ''}`}
                onClick={() => setActiveNode('source_products')}
              >
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
                data-node-id="ingest_orders"
                className={`node-card ${nodeStatuses['ingest_orders'] || 'idle'} ${activeNode === 'ingest_orders' ? 'selected' : ''}`}
                onClick={() => setActiveNode('ingest_orders')}
              >
                <div className="node-header">
                  <div className="node-icon-wrapper"><Server size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Ingest Orders</h3>
                <p className="node-subtitle">orders_{`{date}`}.csv</p>
              </div>

              <div 
                data-node-id="ingest_events"
                className={`node-card ${nodeStatuses['ingest_events'] || 'idle'} ${activeNode === 'ingest_events' ? 'selected' : ''}`}
                onClick={() => setActiveNode('ingest_events')}
              >
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
                data-node-id="validate_schema"
                className={`node-card ${nodeStatuses['validate_schema'] || 'idle'} ${activeNode === 'validate_schema' ? 'selected' : ''}`}
                onClick={() => setActiveNode('validate_schema')}
              >
                <div className="node-header">
                  <div className="node-icon-wrapper"><FileCode2 size={14} /></div>
                  <div className="node-status-dot"></div>
                </div>
                <h3 className="node-title">Validate Schema</h3>
                <p className="node-subtitle">Structure Checks</p>
              </div>

              <div 
                data-node-id="validate_quality"
                className={`node-card ${nodeStatuses['validate_quality'] || 'idle'} ${activeNode === 'validate_quality' ? 'selected' : ''}`}
                onClick={() => setActiveNode('validate_quality')}
              >
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
                data-node-id="load_orders_bq"
                className={`node-card ${nodeStatuses['load_orders_bq'] || 'idle'} ${activeNode === 'load_orders_bq' ? 'selected' : ''}`}
                onClick={() => setActiveNode('load_orders_bq')}
              >
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
                data-node-id="bq_row_count_check"
                className={`node-card ${nodeStatuses['bq_row_count_check'] || 'idle'} ${activeNode === 'bq_row_count_check' ? 'selected' : ''}`}
                onClick={() => setActiveNode('bq_row_count_check')}
              >
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
                data-node-id="agent_monitor"
                className={`node-card ${nodeStatuses['agent_monitor'] || 'idle'} ${activeNode === 'agent_monitor' ? 'selected' : ''}`}
                onClick={() => setActiveNode('agent_monitor')}
              >
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

                  <div style={{ fontSize: '11px', lineHeight: 1.4, color: 'var(--text-secondary)' }}>
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
                nodeStatuses[selectedNodeInfo.id] === 'failed' && (
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
                        <pre style={{ margin: 0, padding: '8px', backgroundColor: 'var(--bg-canvas)', border: '1px solid var(--border-color)', borderRadius: '4px', fontFamily: 'var(--mono)', fontSize: '10px', overflowX: 'auto', whiteSpace: 'pre-wrap', color: 'var(--text-primary)' }}>
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
                        {renderDataTypeBadge(type)}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Node step-specific execution trigger */}
              {selectedNodeInfo.type !== 'input' && (
                <div className="node-action-box">
                  <button 
                    className="btn btn-action-secondary"
                    onClick={() => handleRunSingleStep(selectedNodeInfo.id)}
                    disabled={isSimulationActive || nodeStatuses[selectedNodeInfo.id] === 'running'}
                  >
                    <RefreshCw size={12} className={nodeStatuses[selectedNodeInfo.id] === 'running' ? 'animate-spin' : ''} />
                    <span>Run Step Directly</span>
                  </button>
                </div>
              )}
            </div>
          </aside>
        )}

        {/* Collapsible Telemetry Drawer (Frosted) */}
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

            {/* Pipeline telemetry metrics with gradient Area Chart */}
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

                {/* SVG Area chart */}
                <div className="stat-item" style={{ padding: '8px 12px', justifyContent: 'flex-start', gap: '16px' }}>
                  <div className="stat-info" style={{ minWidth: '100px' }}>
                    <h4>Throughput</h4>
                    <p className="stat-val" style={{ fontSize: '13px' }}>7-Day History</p>
                    <div style={{ display: 'flex', gap: '8px', fontSize: '9px', color: '#94a3b8', marginTop: '6px' }}>
                      <span style={{ display: 'flex', alignItems: 'center', gap: '3px' }}>
                        <span style={{ width: '6px', height: '6px', borderRadius: '50%', backgroundColor: 'var(--accent)' }}></span> Orders
                      </span>
                      <span style={{ display: 'flex', alignItems: 'center', gap: '3px' }}>
                        <span style={{ width: '6px', height: '6px', borderRadius: '50%', backgroundColor: 'var(--info)' }}></span> Events
                      </span>
                    </div>
                  </div>
                  
                  {renderStatsChart()}
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
