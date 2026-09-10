import { useState, useEffect } from 'react';
import { 
  Activity, 
  AlertTriangle, 
  CheckCircle2, 
  Database, 
  FileCode2, 
  ShieldAlert, 
  Play, 
  X, 
  RefreshCw, 
  UserCheck, 
  Server, 
  AlertCircle,
  Sun,
  Moon,
  ArrowRight,
  GitBranch,
  Layers,
  ShieldCheck,
  Cpu,
  Clock,
  Check,
  Eye,
  BarChart2
} from 'lucide-react';

interface Incident {
  id: string;
  timestamp: string;
  task_id: string;
  dataset: string;
  fault_category: string;
  severity: 'high' | 'medium';
  status: 'pending_approval' | 'remediated' | 'escalated';
  observed: string;
  expected: string;
  evidence: string;
  hypothesis: string;
  confidence: string;
  blast_radius: string;
  action: 'AUTO_FIX' | 'ESCALATE';
  remediation: string;
  verification: string;
  possible_root_causes?: string[];
  remediation_status?: string;
  verification_status?: string;
  ai_mode?: string;
  diagnosis_source?: string;
}

interface LogLine {
  time: string;
  level: 'info' | 'warn' | 'error' | 'success';
  text: string;
}

interface ProfileSummary {
  dataset: string;
  row_count: number;
  null_pct: number;
  duplicate_pct: number;
  freshness_sla: string;
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
  stage: 'data_sources' | 'ingestion' | 'validation' | 'transformation' | 'load' | 'monitoring';
  type: 'input' | 'ingest' | 'validate' | 'transform' | 'load' | 'check' | 'monitor';
  details: NodeDetails;
}

function App() {
  const [theme, setTheme] = useState<'light' | 'dark'>('light');
  const [activeTab, setActiveTab] = useState<'logs' | 'remediations' | 'stats' | 'profiling'>('logs');
  const [telemetryOpen, setTelemetryOpen] = useState(true);
  const [pipelineStatus, setPipelineStatus] = useState<'healthy' | 'anomaly' | 'failed'>('healthy');
  const [activeFault, setActiveFault] = useState<string | null>(null);
  const [isProcessing, setIsProcessing] = useState<string | null>(null);
  const [activeNode, setActiveNode] = useState<string>('ingest_orders');
  const [viewMode, setViewMode] = useState<'overview' | 'canvas'>('overview');
  const [selectedHealingStep, setSelectedHealingStep] = useState<'FAILURE' | 'INCIDENT' | 'DIAGNOSIS' | 'POLICY_GATE' | 'VERIFICATION' | 'RESOLVED'>('DIAGNOSIS');

  const [nodeStatuses, setNodeStatuses] = useState<Record<string, 'healthy' | 'warning' | 'failed' | 'idle' | 'running'>>({});
  const [nodeDurations] = useState<Record<string, string>>({
    source_customers: 'static',
    source_products: 'static',
    ingest_dimensions: '0.8s',
    ingest_orders: '1.2s',
    ingest_events: '1.5s',
    validate_schema: '0.6s',
    validate_quality: '1.1s',
    transform_data: '1.4s',
    load_data: '2.1s',
    agent_monitoring: '0.5s'
  });
  const [isSimulationActive, setIsSimulationActive] = useState(false);
  const [connections, setConnections] = useState<Array<{ from: string, to: string, path: string, status: string }>>([]);

  // Pipeline Run Summary (Item 11)
  const [runSummary] = useState({
    runId: 'run-20260601-0200',
    startTime: '02:00:00 UTC',
    endTime: '02:02:15 UTC',
    duration: '135s',
    tasksPassed: 8,
    tasksFailed: 0,
    recordsProcessed: 2060,
    recordsRejected: 0,
    incidentsCreated: 1,
    autoFixes: 1,
    escalations: 0,
    finalStatus: 'SUCCESS'
  });

  // Data Profiling State (Item 1)
  const [profiles] = useState<ProfileSummary[]>([
    { dataset: 'orders', row_count: 300, null_pct: 0.2, duplicate_pct: 0.0, freshness_sla: '< 26h (Healthy)' },
    { dataset: 'events', row_count: 1200, null_pct: 0.1, duplicate_pct: 0.0, freshness_sla: '< 6h (Healthy)' },
    { dataset: 'customers', row_count: 500, null_pct: 0.0, duplicate_pct: 0.0, freshness_sla: 'Static Dimension' },
    { dataset: 'products', row_count: 60, null_pct: 0.0, duplicate_pct: 0.0, freshness_sla: 'Static Dimension' },
  ]);

  // Metrics overview
  const [metrics, setMetrics] = useState({
    lastRun: '2026-06-01 02:00 UTC',
    status: 'SUCCESS',
    activeIncidents: 1,
    successfulRuns: 18,
    failedRuns: 2,
    remediatedIncidents: 2
  });

  // Incidents state
  const [incidents, setIncidents] = useState<Incident[]>([
    {
      id: "INC-20260601-1092",
      timestamp: "2026-06-01 02:05:12",
      task_id: "validate_quality",
      dataset: "orders",
      fault_category: "DUPLICATE_INGESTION",
      severity: "medium",
      status: "pending_approval",
      observed: "Staged orders: 1,450 rows | Expected range: 240-360 rows (+302% duplicate spike)",
      expected: "Unique primary keys (order_id) with row count within 240-360 baseline",
      evidence: "1,150 duplicate records identified by order_id grouping in staged daily batch",
      hypothesis: "Upstream scheduler retry triggered duplicate file transfer into staging folder",
      confidence: "98.5%",
      blast_radius: "Medium (affects downstream fct_orders metrics and daily aggregate totals)",
      action: "AUTO_FIX",
      remediation: "Execute idempotent DELETE on staged duplicates by primary key order_id",
      verification: "Assert unique order_id count matches baseline (300 rows)",
      possible_root_causes: [
        "Upstream scheduler retry triggered duplicate file export into staging",
        "Source database CDC log re-play without deduplication window",
        "Manual file upload of historical batch"
      ],
      remediation_status: "PENDING",
      verification_status: "PENDING"
    }
  ]);

  const [remediations, setRemediations] = useState([
    { id: "rem-091", time: "2026-06-01 02:06:00", fault: "Duplicate Ingestion", target: "orders", method: "Idempotent Deduplication", duration: "3.8s", status: "success" },
    { id: "rem-085", time: "2026-05-31 02:05:18", fault: "Staleness Warning", target: "events", method: "Sensor Recrawl", duration: "8.5s", status: "success" }
  ]);

  const [consoleLogs, setConsoleLogs] = useState<LogLine[]>([
    { time: "02:00:01", level: "info", text: "Airflow LocalExecutor initialized for DAG self_healing_pipeline." },
    { time: "02:00:03", level: "success", text: "Connected to metadata database." },
    { time: "02:00:05", level: "info", text: "DAG 'self_healing_pipeline' scheduled run started for ds=2026-06-01." },
    { time: "02:00:10", level: "info", text: "Ingestion stage started: dimensions, orders, events in parallel." },
    { time: "02:00:12", level: "success", text: "[DATA PROFILER] Orders: 300 rows, 0.2% nulls, 0.0% duplicates." },
    { time: "02:00:15", level: "success", text: "[DATA PROFILER] Events: 1,200 rows, 0.1% nulls, 0.0% duplicates." }
  ]);

  // 6 Supported Fault Scenarios
  const faultScenarios = [
    {
      id: 'schema_drift',
      name: 'Schema Drift',
      desc: 'Extra string column in products CSV',
      risk: 'high',
      task: 'validate_schema',
      dataset: 'products',
      action: 'ESCALATE' as const
    },
    {
      id: 'volume_drop',
      name: 'Volume Drop',
      desc: 'Low row count (12 rows vs 240-360 expected)',
      risk: 'high',
      task: 'validate_quality',
      dataset: 'orders',
      action: 'ESCALATE' as const
    },
    {
      id: 'null_spike',
      name: 'Null Spike',
      desc: '14.5% null order_total values (>2% limit)',
      risk: 'high',
      task: 'validate_quality',
      dataset: 'orders',
      action: 'ESCALATE' as const
    },
    {
      id: 'duplicate_ingestion',
      name: 'Duplicate Ingest',
      desc: '1,450 rows staged with repeated order_ids',
      risk: 'medium',
      task: 'validate_quality',
      dataset: 'orders',
      action: 'AUTO_FIX' as const
    },
    {
      id: 'referential_break',
      name: 'Referential Break',
      desc: 'Missing customer_id reference C-9988 in dim',
      risk: 'high',
      task: 'validate_quality',
      dataset: 'orders',
      action: 'ESCALATE' as const
    },
    {
      id: 'staleness',
      name: 'Staleness SLA',
      desc: 'Batch timestamp exceeds 26h SLA threshold',
      risk: 'medium',
      task: 'validate_quality',
      dataset: 'events',
      action: 'AUTO_FIX' as const
    }
  ];

  const pipelineNodes: PipelineNode[] = [
    {
      id: "source_customers",
      title: "customers.csv",
      subtitle: "Dimension Raw Source",
      stage: "data_sources",
      type: "input",
      details: {
        source: "Local Storage",
        path: "data/customers.csv",
        primaryKey: "customer_id",
        freshnessSla: "Static Dimension",
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
      stage: "data_sources",
      type: "input",
      details: {
        source: "Local Storage",
        path: "data/products.csv",
        primaryKey: "product_id",
        freshnessSla: "Static Dimension",
        schema: {
          product_id: "string",
          name: "string",
          category: "string",
          price: "float"
        }
      }
    },
    {
      id: "ingest_dimensions",
      title: "Ingest Dimensions",
      subtitle: "Task: python_callable",
      stage: "ingestion",
      type: "ingest",
      details: {
        description: "Ingests raw customers.csv and products.csv files into local staging directory.",
        path: "data/staging/stg_customers.csv",
        primaryKey: "customer_id",
        freshnessSla: "Static Dimension Load"
      }
    },
    {
      id: "ingest_orders",
      title: "Ingest Orders",
      subtitle: "Task: python_callable",
      stage: "ingestion",
      type: "ingest",
      details: {
        description: "Ingests the daily orders batch file for execution date into staging.",
        path: "data/orders/orders_{ds}.csv",
        primaryKey: "order_id",
        freshnessSla: "Max 26 Hours"
      }
    },
    {
      id: "ingest_events",
      title: "Ingest Events",
      subtitle: "Task: python_callable",
      stage: "ingestion",
      type: "ingest",
      details: {
        description: "Pulls clickstream event JSONL stream logs and stages them locally.",
        path: "data/events/events_{ds}.jsonl",
        primaryKey: "event_id",
        freshnessSla: "Max 6 Hours"
      }
    },
    {
      id: "validate_schema",
      title: "Validate Schema",
      subtitle: "Task: python_callable",
      stage: "validation",
      type: "validate",
      details: {
        description: "Compares current staged table schemas against pipeline_config.yaml contract definitions.",
        rule: "Strict column presence, names, and data types check. Raises SCHEMA_DRIFT error.",
        activeRules: "customers schema, products schema, orders schema, events schema"
      }
    },
    {
      id: "validate_quality",
      title: "Validate Quality",
      subtitle: "Task: python_callable",
      stage: "validation",
      type: "validate",
      details: {
        description: "Enforces row volume bounds, null rate thresholds, key duplication, and referential integrity.",
        nullTolerance: "order_total: <= 2.0%, customer_id: 0.0%",
        range: "orders: 240 - 360 rows/day | events: 900 - 1500 rows/day",
        referentialChecks: "orders.customer_id -> customers.customer_id, events.customer_id -> customers.customer_id"
      }
    },
    {
      id: "transform_data",
      title: "Transform Data",
      subtitle: "Task: python_callable",
      stage: "transformation",
      type: "transform",
      details: {
        description: "Deduplicates records by primary key and standardizes date/timestamp formats into analytical models.",
        targetTable: "fct_orders, fct_events, dim_customers, dim_products",
        mode: "Clean transform & format"
      }
    },
    {
      id: "load_data",
      title: "Load to Storage / BQ",
      subtitle: "Task: python_callable",
      stage: "load",
      type: "load",
      details: {
        targetTable: "fct_orders, fct_events",
        partitionBy: "order_ts (DAY)",
        clusterBy: "customer_id",
        mode: "WRITE_TRUNCATE (idempotency safety check)"
      }
    },
    {
      id: "agent_monitoring",
      title: "Agent Monitoring",
      subtitle: "Task: python_callable",
      stage: "monitoring",
      type: "monitor",
      details: {
        description: "Logs pipeline execution metrics, calculates row counts, records status heartbeat.",
        activeRules: "remediation_policy: duplicate_ingestion=auto_fix, volume_anomaly_spike=auto_fix"
      }
    }
  ];

  useEffect(() => {
    if (isSimulationActive) return;

    const getNodeStatus = (nodeId: string): 'healthy' | 'warning' | 'failed' | 'idle' | 'running' => {
      if (pipelineStatus === 'healthy') {
        if (nodeId === 'validate_quality' && incidents.some(i => i.status === 'pending_approval' && i.action === 'AUTO_FIX')) {
          return 'warning';
        }
        return 'healthy';
      }

      if (activeFault === 'schema_drift') {
        if (nodeId === 'validate_schema') return 'failed';
        const downstream = ['validate_quality', 'transform_data', 'load_data', 'agent_monitoring'];
        if (downstream.includes(nodeId)) return 'idle';
        return 'healthy';
      }

      if (activeFault && activeFault !== 'schema_drift') {
        if (nodeId === 'validate_quality') return 'failed';
        const downstream = ['transform_data', 'load_data', 'agent_monitoring'];
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
      ingest_dimensions: getNodeStatus('ingest_dimensions'),
      ingest_orders: getNodeStatus('ingest_orders'),
      ingest_events: getNodeStatus('ingest_events'),
      validate_schema: getNodeStatus('validate_schema'),
      validate_quality: getNodeStatus('validate_quality'),
      transform_data: getNodeStatus('transform_data'),
      load_data: getNodeStatus('load_data'),
      agent_monitoring: getNodeStatus('agent_monitoring'),
    };
    setNodeStatuses(initial);
  }, [activeFault, pipelineStatus, incidents, isSimulationActive]);

  // Sync real incident reports from FastAPI backend if available
  useEffect(() => {
    const fetchLiveIncidents = async () => {
      try {
        const res = await fetch('http://localhost:8000/api/v1/incidents');
        if (!res.ok) return;
        const data = await res.json();
        if (Array.isArray(data) && data.length > 0) {
          const mapped: Incident[] = data.map((item: any) => ({
            id: item.incident_id || item.id || `INC-${Date.now()}`,
            timestamp: item.timestamp || new Date().toISOString().substring(0, 19),
            task_id: item.task_id || item.failed_check || item.check || 'validate_quality',
            dataset: item.dataset || 'orders',
            fault_category: (item.fault_category || item.category || 'UNKNOWN').toUpperCase(),
            severity: (item.severity || 'medium').toLowerCase() as 'high' | 'medium',
            status: (item.status || 'pending_approval').toLowerCase() as any,
            observed: item.observed || item.observed_value || 'Pipeline failure detected',
            expected: item.expected || item.expected_value || 'Expected healthy contract',
            evidence: typeof item.evidence === 'object' ? JSON.stringify(item.evidence) : String(item.evidence || ''),
            hypothesis: item.hypothesis || 'AI Failure Diagnosis pending',
            confidence: item.confidence ? `${Math.round(item.confidence * 100)}%` : '92%',
            blast_radius: item.blast_radius || 'Staged datasets impacted',
            action: item.action || 'ESCALATE',
            remediation: typeof item.remediation === 'object' ? (item.remediation?.rationale || 'Remediation plan') : String(item.remediation || ''),
            verification: typeof item.verification === 'object' ? JSON.stringify(item.verification) : String(item.verification || 'Pending verification'),
            possible_root_causes: item.possible_root_causes || [],
            remediation_status: item.remediation_status || 'PENDING',
            verification_status: item.verification_status || 'PENDING',
            ai_mode: item.evidence?.ai_mode,
            diagnosis_source: item.evidence?.diagnosis_source || (item.evidence?.ai_mode?.includes('OLLAMA') ? 'OLLAMA' : undefined),
          }));
          setIncidents(mapped);
          setMetrics(prev => ({
            ...prev,
            activeIncidents: mapped.filter(i => i.status === 'pending_approval').length,
            remediatedIncidents: mapped.filter(i => i.status === 'remediated').length
          }));
        }
      } catch (err) {
        // Backend not running locally or CORS; keep local state intact
      }
    };
    fetchLiveIncidents();
  }, []);

  const updateConnections = () => {
    const flowElement = document.querySelector('.pipeline-flow');
    if (!flowElement) return;
    const flowRect = flowElement.getBoundingClientRect();
    
    const newConnections: Array<{ from: string, to: string, path: string, status: string }> = [];
    const linkPairs = [
      { from: 'source_customers', to: 'ingest_dimensions' },
      { from: 'source_products', to: 'ingest_dimensions' },
      { from: 'ingest_dimensions', to: 'validate_schema' },
      { from: 'ingest_orders', to: 'validate_schema' },
      { from: 'ingest_events', to: 'validate_schema' },
      { from: 'validate_schema', to: 'validate_quality' },
      { from: 'validate_quality', to: 'transform_data' },
      { from: 'transform_data', to: 'load_data' },
      { from: 'load_data', to: 'agent_monitoring' }
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

  useEffect(() => {
    const timer = setTimeout(() => {
      updateConnections();
    }, 120);
    
    window.addEventListener('resize', updateConnections);
    return () => {
      clearTimeout(timer);
      window.removeEventListener('resize', updateConnections);
    };
  }, [activeNode, pipelineStatus, activeFault, nodeStatuses, telemetryOpen, theme, viewMode]);

  const addLog = (level: LogLine['level'], text: string) => {
    const time = new Date().toTimeString().split(' ')[0];
    setConsoleLogs(prev => [...prev, { time, level, text }]);
  };

  const runPipelineSimulation = () => {
    if (isSimulationActive) return;
    setIsSimulationActive(true);
    setPipelineStatus('healthy'); 
    
    const resetStates: Record<string, 'healthy' | 'warning' | 'failed' | 'idle' | 'running'> = {
      source_customers: 'healthy',
      source_products: 'healthy',
      ingest_dimensions: 'idle',
      ingest_orders: 'idle',
      ingest_events: 'idle',
      validate_schema: 'idle',
      validate_quality: 'idle',
      transform_data: 'idle',
      load_data: 'idle',
      agent_monitoring: 'idle'
    };
    setNodeStatuses(resetStates);
    setConsoleLogs([]);
    
    addLog("info", "Starting E2E self-healing data pipeline run for ds=2026-06-01...");
    addLog("info", "Executing Stage 1: INGESTION & DATA PROFILING...");
    
    setTimeout(() => {
      setNodeStatuses(prev => ({ ...prev, ingest_dimensions: 'running', ingest_orders: 'running', ingest_events: 'running' }));
      addLog("info", "Task: ingest_dimensions -> Ingesting customers.csv & products.csv...");
      addLog("info", "Task: ingest_orders -> Staging orders_2026-06-01.csv...");
      addLog("info", "Task: ingest_events -> Parsing events_2026-06-01.jsonl stream...");
      
      setTimeout(() => {
        setNodeStatuses(prev => ({ ...prev, ingest_dimensions: 'healthy', ingest_orders: 'healthy', ingest_events: 'healthy', validate_schema: 'running' }));
        addLog("success", "INGESTION complete: 500 customers, 60 products, 300 orders, 1,200 events staged.");
        addLog("success", "[DATA PROFILER] Profile generated: 0.2% null rate, 0.0% duplicates.");
        addLog("info", "Executing Stage 2: SCHEMA VALIDATION...");
        
        setTimeout(() => {
          if (activeFault === 'schema_drift') {
            setNodeStatuses(prev => ({ ...prev, validate_schema: 'failed' }));
            setPipelineStatus('failed');
            addLog("error", "Task validate_schema failed! Schema drift detected in products.csv.");
            addLog("warn", "Field 'price' expected FLOAT, got STRING (e.g. '$14.99').");
            addLog("info", "Invoking AI Diagnostic Engine hook...");
            
            const newInc: Incident = {
              id: `INC-20260601-${Math.floor(1000 + Math.random() * 9000)}`,
              timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
              task_id: "validate_schema",
              dataset: "products",
              fault_category: "SCHEMA_DRIFT",
              severity: "high",
              status: "pending_approval",
              observed: "Field 'price' in products.csv failed type check. Expected FLOAT, found STRING.",
              expected: "Schema contract: price FLOAT, product_id STRING, category STRING",
              evidence: "Unparsed string values '$14.99' in column 4 across 100% of rows",
              hypothesis: "Upstream product catalog system export updated price formatting to include currency symbol",
              confidence: "99.1%",
              blast_radius: "High (prevents numeric aggregations and price calculations in downstream tables)",
              action: "ESCALATE",
              remediation: "Escalate to Data Engineering team for schema contract update or source API patch",
              verification: "Manual verification after source column format correction",
              possible_root_causes: [
                "Upstream product catalog system updated export format",
                "Source database column migration from float to string"
              ],
              remediation_status: "ESCALATED",
              verification_status: "PENDING"
            };
            setIncidents(prev => [newInc, ...prev.filter(i => i.task_id !== 'validate_schema')]);
            setActiveNode("validate_schema");
            setIsSimulationActive(false);
            return;
          }
          
          setNodeStatuses(prev => ({ ...prev, validate_schema: 'healthy', validate_quality: 'running' }));
          addLog("success", "Schema validation PASSED. All column structures match contract.");
          addLog("info", "Executing Stage 2 (cont): QUALITY VALIDATION...");
          
          setTimeout(() => {
            if (activeFault === 'null_spike') {
              setNodeStatuses(prev => ({ ...prev, validate_quality: 'failed' }));
              setPipelineStatus('failed');
              addLog("error", "Task validate_quality failed! Null spike detected in order_total.");
              addLog("warn", "Field 'order_total' contains 14.5% null values (threshold <= 2.0%).");
              addLog("info", "Invoking AI Diagnostic Engine hook...");
              
              const newInc: Incident = {
                id: `INC-20260601-${Math.floor(1000 + Math.random() * 9000)}`,
                timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
                task_id: "validate_quality",
                dataset: "orders",
                fault_category: "NULL_SPIKE",
                severity: "high",
                status: "pending_approval",
                observed: "Order total null rate reached 14.5% (threshold limit: 2.0%)",
                expected: "order_total null rate <= 2.0% across all daily staged records",
                evidence: "44 null order_total values out of 300 staged orders",
                hypothesis: "Database extraction query omitted default fallback for pending payments",
                confidence: "95.4%",
                blast_radius: "High (affects revenue analytics and daily order volume reports)",
                action: "ESCALATE",
                remediation: "Halt transaction pipeline, quarantine null rows, escalate to finance engineering",
                verification: "Re-run validate_quality after null imputation or source fix",
                possible_root_causes: [
                  "Extraction query omitted default fallback for pending payments",
                  "Upstream payload truncation during network export"
                ],
                remediation_status: "ESCALATED",
                verification_status: "PENDING"
              };
              setIncidents(prev => [newInc, ...prev.filter(i => i.task_id !== 'validate_quality')]);
              setActiveNode("validate_quality");
              setIsSimulationActive(false);
              return;
            }
            
            setNodeStatuses(prev => ({ ...prev, validate_quality: 'healthy', transform_data: 'running' }));
            addLog("success", "Data quality assertions PASSED: Row bounds, null rates, and referential keys valid.");
            addLog("info", "Executing Stage 3: TRANSFORMATION (deduplication & timestamp formatting)...");
            
            setTimeout(() => {
              setNodeStatuses(prev => ({ ...prev, transform_data: 'healthy', load_data: 'running' }));
              addLog("success", "TRANSFORMATION complete: Clean analytical models generated.");
              addLog("info", "Executing Stage 4: LOAD (committing tables to Storage / BigQuery)...");
              
              setTimeout(() => {
                setNodeStatuses(prev => ({ ...prev, load_data: 'healthy', agent_monitoring: 'running' }));
                addLog("success", "LOAD complete: 300 orders & 1,200 events committed to target storage.");
                addLog("info", "Executing Stage 5: MONITORING (logging metrics & heartbeat)...");
                
                setTimeout(() => {
                  setNodeStatuses(prev => ({ ...prev, agent_monitoring: 'healthy' }));
                  setPipelineStatus('healthy');
                  addLog("success", "E2E Self-Healing Pipeline run completed with status SUCCESS!");
                  setIsSimulationActive(false);
                }, 1000);
              }, 1000);
            }, 1000);
          }, 1000);
        }, 1000);
      }, 1000);
    }, 1200);
  };

  const handleInjectFault = (faultId: string) => {
    if (activeFault || isSimulationActive) return; 

    const fault = faultScenarios.find(f => f.id === faultId);
    if (!fault) return;

    setActiveFault(faultId);
    setPipelineStatus('failed');

    const newIncident: Incident = {
      id: `INC-20260601-${Math.floor(1000 + Math.random() * 9000)}`,
      timestamp: new Date().toISOString().replace('T', ' ').substring(0, 19),
      task_id: fault.task,
      dataset: fault.dataset,
      fault_category: fault.name.toUpperCase().replace(' ', '_'),
      severity: fault.risk === 'high' ? 'high' : 'medium',
      status: "pending_approval",
      observed: `Fault trigger: ${fault.name} in ${fault.dataset}. ${fault.desc}`,
      expected: "Healthy baseline contract assertions passed",
      evidence: `Diagnostic check triggered for ${fault.id}: ${fault.desc}`,
      hypothesis: `Injected simulation scenario for ${fault.name}`,
      confidence: "97.8%",
      blast_radius: `${fault.risk.toUpperCase()} risk impact on ${fault.dataset} downstream analytics`,
      action: fault.action,
      remediation: fault.action === 'AUTO_FIX' 
        ? `Execute automated self-healing fix for ${fault.name}`
        : `Escalate incident to on-call engineer for manual review`,
      verification: "Assert contract rules on target table",
      possible_root_causes: [
        `Simulation rule trigger for ${fault.name}`,
        `Upstream batch irregularity in ${fault.dataset}`
      ],
      remediation_status: "PENDING",
      verification_status: "PENDING"
    };

    setIncidents(prev => [newIncident, ...prev.filter(i => i.task_id !== fault.task)]);
    setActiveNode(fault.task);
    setTelemetryOpen(true);
    setActiveTab('logs');
    
    addLog("error", `Fault Injected: ${fault.name} in task ${fault.task}!`);
    addLog("warn", fault.desc);
    addLog("info", `AI Policy Gate Evaluated: Action set to ${fault.action}`);
  };

  const handleApproveRemediation = (id: string) => {
    const incident = incidents.find(i => i.id === id);
    if (!incident) return;

    setIsProcessing(id);
    addLog("info", `User approved self-healing remediation for incident ${id}.`);
    addLog("info", `Executing action: ${incident.remediation}`);

    setTimeout(() => {
      addLog("success", "Remediation engine initialized.");
      addLog("info", `Applying automated fix for ${incident.fault_category} on dataset '${incident.dataset}'...`);
      
      setTimeout(() => {
        addLog("success", "Fix executed successfully. Post-remediation verification running...");
        
        setTimeout(() => {
          addLog("success", "Verification PASSED. All data quality indicators restored to HEALTHY.");
          addLog("success", `Pipeline resumed. Incident ${id} status updated to REMEDIATED.`);

          setIncidents(prev => prev.map(inc => inc.id === id ? { 
            ...inc, 
            status: "remediated",
            remediation_status: "SUCCESS",
            verification_status: "PASSED"
          } : inc));
          
          setRemediations(prev => [
            {
              id: `rem-${Math.floor(100 + Math.random() * 900)}`,
              time: new Date().toISOString().replace('T', ' ').substring(0, 19),
              fault: incident.fault_category,
              target: incident.dataset,
              method: incident.action === 'AUTO_FIX' ? "Automated Self-Healing" : "Guided Patch",
              duration: "3.2s",
              status: "success"
            },
            ...prev
          ]);

          setMetrics(prev => ({
            ...prev,
            activeIncidents: Math.max(0, prev.activeIncidents - 1),
            remediatedIncidents: prev.remediatedIncidents + 1
          }));

          setIsProcessing(null);
          setActiveFault(null);
          setPipelineStatus('healthy');
        }, 1000);
      }, 1000);
    }, 1000);
  };

  const handleDeclineIncident = (id: string) => {
    setIncidents(prev => prev.map(inc => inc.id === id ? { 
      ...inc, 
      status: "escalated",
      remediation_status: "ESCALATED_HUMAN_REVIEW",
      verification_status: "PENDING_HUMAN"
    } : inc));
    addLog("warn", `Incident ${id} escalated to human engineer review queue.`);
    setActiveFault(null);
    setPipelineStatus('healthy');
  };

  const renderDataTypeBadge = (type: string) => {
    return <span className="schema-type">{type.toUpperCase()}</span>;
  };

  const selectedNodeInfo = pipelineNodes.find(n => n.id === activeNode);
  const activeIncident = incidents.find(i => i.task_id === activeNode && i.status === 'pending_approval');

  return (
    <div className={`app-container ${theme}-theme`}>
      {/* Sidebar Nav */}
      <aside className="sidebar">
        <div className="top-part">
          <div className="logo-section">
            <Activity className="logo-icon" size={20} />
            <span className="logo-text">Data Pipeline AI</span>
          </div>

          <nav className="nav-links">
            <div 
              className={`nav-item ${viewMode === 'overview' ? 'active' : ''}`}
              onClick={() => setViewMode('overview')}
            >
              <GitBranch size={16} />
              Live System Flow
            </div>
            <div 
              className={`nav-item ${viewMode === 'canvas' ? 'active' : ''}`}
              onClick={() => setViewMode('canvas')}
            >
              <Layers size={16} />
              DAG Node Canvas
            </div>
            <div 
              className="nav-item" 
              onClick={() => {
                setTelemetryOpen(true); 
                setActiveTab('profiling');
              }}
            >
              <BarChart2 size={16} />
              Data Profiling
            </div>
            <div 
              className="nav-item" 
              onClick={() => {
                setTelemetryOpen(true); 
                setActiveTab('stats');
              }}
            >
              <Database size={16} />
              Telemetry & Volume
            </div>
            <div 
              className="nav-item"
              onClick={() => {
                setTelemetryOpen(true);
                setActiveTab('remediations');
              }}
            >
              <ShieldCheck size={16} />
              Remediation History
            </div>
          </nav>
        </div>

        <div>
          <div className="sidebar-controls">
            <button 
              className="theme-toggle-btn"
              onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
            >
              {theme === 'light' ? (
                <>
                  <Moon size={13} />
                  <span>Dark Mode</span>
                </>
              ) : (
                <>
                  <Sun size={13} />
                  <span>Light Mode</span>
                </>
              )}
            </button>
          </div>
          
          <div className="sidebar-footer">
            <div>Self-Healing Pipeline</div>
            <div style={{ color: 'var(--accent)', marginTop: '2px', fontWeight: 600 }}>Airflow LocalExecutor</div>
          </div>
        </div>
      </aside>

      {/* Main Workspace Frame */}
      <main className="main-content">
        {/* Header bar */}
        <header className="header">
          <div className="header-title">
            <h1>Self-Healing Data Pipeline Control Center</h1>
            <p>Production Airflow DAG Monitor & AI Self-Healing Engine</p>
          </div>

          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <div className="view-mode-tabs">
              <button 
                className={`view-mode-btn ${viewMode === 'overview' ? 'active' : ''}`}
                onClick={() => setViewMode('overview')}
              >
                Overview Flow
              </button>
              <button 
                className={`view-mode-btn ${viewMode === 'canvas' ? 'active' : ''}`}
                onClick={() => setViewMode('canvas')}
              >
                Interactive Canvas
              </button>
            </div>

            <div className={`status-badge ${pipelineStatus === 'healthy' ? 'healthy' : pipelineStatus === 'anomaly' ? 'anomaly' : 'failed'}`}>
              <span className="pulse-dot"></span>
              {pipelineStatus === 'healthy' && 'Pipeline Healthy'}
              {pipelineStatus === 'anomaly' && 'Anomaly Detected'}
              {pipelineStatus === 'failed' && 'Task Failure'}
            </div>
          </div>
        </header>

        {/* Pipeline Run Summary Box (Item 11) */}
        <div className="run-summary-container">
          <div className="run-summary-item">
            <span className="sum-label"><Clock size={10} /> Run ID</span>
            <span className="sum-val">{runSummary.runId}</span>
          </div>
          <div className="run-summary-item">
            <span className="sum-label">Duration</span>
            <span className="sum-val">{runSummary.duration}</span>
          </div>
          <div className="run-summary-item">
            <span className="sum-label">Tasks Passed/Failed</span>
            <span className="sum-val" style={{ color: 'var(--healthy)' }}>{runSummary.tasksPassed} / {runSummary.tasksFailed}</span>
          </div>
          <div className="run-summary-item">
            <span className="sum-label">Records Processed</span>
            <span className="sum-val">{runSummary.recordsProcessed}</span>
          </div>
          <div className="run-summary-item">
            <span className="sum-label">Auto-Fix / Escalate</span>
            <span className="sum-val" style={{ color: 'var(--accent)' }}>{runSummary.autoFixes} / {runSummary.escalations}</span>
          </div>
          <div className="run-summary-item">
            <span className="sum-label">Final Status</span>
            <span className="sum-val" style={{ color: 'var(--healthy)' }}>{runSummary.finalStatus}</span>
          </div>
        </div>

        {/* Compact Metrics Summary Bar */}
        <div className="metrics-summary-bar">
          <div className="metric-card">
            <div className="metric-title"><Clock size={11} /> Last Pipeline Run</div>
            <div className="metric-val" style={{ fontSize: '13px' }}>{metrics.lastRun}</div>
          </div>
          <div className="metric-card">
            <div className="metric-title"><Activity size={11} /> Current Status</div>
            <div className="metric-val" style={{ color: pipelineStatus === 'healthy' ? 'var(--healthy)' : 'var(--failed)' }}>
              {pipelineStatus.toUpperCase()}
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-title"><AlertTriangle size={11} /> Active Incidents</div>
            <div className="metric-val" style={{ color: incidents.filter(i => i.status === 'pending_approval').length > 0 ? '#f59e0b' : 'var(--text-primary)' }}>
              {incidents.filter(i => i.status === 'pending_approval').length}
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-title"><CheckCircle2 size={11} /> Successful Runs</div>
            <div className="metric-val" style={{ color: 'var(--healthy)' }}>{metrics.successfulRuns}</div>
          </div>
          <div className="metric-card">
            <div className="metric-title"><AlertCircle size={11} /> Failed Runs</div>
            <div className="metric-val" style={{ color: metrics.failedRuns > 0 ? '#ef4444' : 'var(--text-primary)' }}>
              {metrics.failedRuns}
            </div>
          </div>
          <div className="metric-card">
            <div className="metric-title"><ShieldCheck size={11} /> Remediated</div>
            <div className="metric-val" style={{ color: 'var(--accent)' }}>{metrics.remediatedIncidents}</div>
          </div>
        </div>

        {/* Fault Injection Panel */}
        <div className="live-flow-container" style={{ padding: '12px 16px', marginBottom: '16px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
            <span style={{ fontSize: '11px', fontWeight: 700, color: 'var(--text-primary)', textTransform: 'uppercase', letterSpacing: '0.5px', display: 'flex', alignItems: 'center', gap: '6px' }}>
              <Cpu size={13} /> Fault Injection Simulator (6 Scenarios)
            </span>
            <div style={{ display: 'flex', gap: '8px' }}>
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
                    <span>Trigger DAG Run</span>
                  </>
                )}
              </button>
            </div>
          </div>

          <div className="fault-grid-container">
            {faultScenarios.map(fault => (
              <button
                key={fault.id}
                className={`fault-card-btn ${activeFault === fault.id ? 'active' : ''}`}
                onClick={() => handleInjectFault(fault.id)}
                disabled={activeFault !== null || isSimulationActive}
              >
                <div className="fault-head">
                  <span className="fault-name">{fault.name}</span>
                  <span className={`risk-pill ${fault.risk}`}>{fault.risk}</span>
                </div>
                <span className="fault-desc">{fault.desc}</span>
              </button>
            ))}
          </div>
        </div>

        {/* VIEW MODE 1: Live Pipeline Flow View */}
        {viewMode === 'overview' && (
          <div className="live-flow-container">
            <div className="live-flow-header">
              <div>
                <h2><GitBranch size={16} color="var(--accent)" /> System Architecture & Extended Pipeline Flow</h2>
                <p>Complete end-to-end data ingestion, validation, and self-healing lifecycle</p>
              </div>
            </div>

            <div className="flow-diagram-wrapper">
              <div className="flow-path-row">
                <span className="flow-path-label success">Primary Flow</span>
                
                <div className="flow-node-step">
                  <div className="flow-step-box">
                    <span className="step-title">DATA SOURCES</span>
                    <span className="step-sub">CSV / JSONL</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step">
                  <div className="flow-step-box active">
                    <span className="step-title">INGESTION</span>
                    <span className="step-sub">Stage & Profile</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step">
                  <div className="flow-step-box">
                    <span className="step-title">SCHEMA VALIDATION</span>
                    <span className="step-sub">Contract Checks</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step">
                  <div className="flow-step-box">
                    <span className="step-title">QUALITY VALIDATION</span>
                    <span className="step-sub">Null / Volume / Keys</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step">
                  <div className="flow-step-box">
                    <span className="step-title">TRANSFORMATION</span>
                    <span className="step-sub">Clean Models</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step">
                  <div className="flow-step-box">
                    <span className="step-title">LOAD</span>
                    <span className="step-sub">Storage / BQ</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step">
                  <div className="flow-step-box success">
                    <span className="step-title">MONITORING</span>
                    <span className="step-sub">Heartbeat Log</span>
                  </div>
                </div>
              </div>

              {/* Self-Healing Branching Path */}
              <div className="flow-path-row" style={{ backgroundColor: 'rgba(0,0,0,0.1)', borderRadius: '8px', padding: '10px' }}>
                <span className="flow-path-label healing">Healing Flow</span>
                
                <div className="flow-node-step flow-step-clickable" onClick={() => setSelectedHealingStep('FAILURE')}>
                  <div className={`flow-step-box danger ${selectedHealingStep === 'FAILURE' ? 'selected-healing-step' : ''}`}>
                    <span className="step-title">FAILURE</span>
                    <span className="step-sub">Validation Assert</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step flow-step-clickable" onClick={() => setSelectedHealingStep('INCIDENT')}>
                  <div className={`flow-step-box ${selectedHealingStep === 'INCIDENT' ? 'selected-healing-step' : ''}`}>
                    <span className="step-title">INCIDENT</span>
                    <span className="step-sub">Failure Record</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step flow-step-clickable" onClick={() => setSelectedHealingStep('DIAGNOSIS')}>
                  <div className={`flow-step-box active ${selectedHealingStep === 'DIAGNOSIS' ? 'selected-healing-step' : ''}`}>
                    <span className="step-title">AI DIAGNOSIS</span>
                    <span className="step-sub">Baseline Compare</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step flow-step-clickable" onClick={() => setSelectedHealingStep('POLICY_GATE')}>
                  <div className={`flow-step-box ${selectedHealingStep === 'POLICY_GATE' ? 'selected-healing-step' : ''}`}>
                    <span className="step-title">POLICY GATE</span>
                    <span className="step-sub">Auto vs Escalate</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step flow-step-clickable" onClick={() => setSelectedHealingStep('VERIFICATION')}>
                  <div className={`flow-step-box ${selectedHealingStep === 'VERIFICATION' ? 'selected-healing-step' : ''}`}>
                    <span className="step-title">VERIFICATION</span>
                    <span className="step-sub">Post-Fix Assert</span>
                  </div>
                  <span className="flow-arrow"><ArrowRight size={14} /></span>
                </div>

                <div className="flow-node-step flow-step-clickable" onClick={() => setSelectedHealingStep('RESOLVED')}>
                  <div className={`flow-step-box success ${selectedHealingStep === 'RESOLVED' ? 'selected-healing-step' : ''}`}>
                    <span className="step-title">RESOLVED</span>
                    <span className="step-sub">Status Green</span>
                  </div>
                </div>
              </div>

              {/* Interactive AI Diagnosis, Policy Gate & Verification Inspection Cards */}
              {incidents.length > 0 && (
                <div className="healing-detail-grid">
                  {/* CARD 1: AI DIAGNOSIS */}
                  <div className={`healing-card ${selectedHealingStep === 'DIAGNOSIS' ? 'selected-healing-step' : ''}`} style={{ borderColor: selectedHealingStep === 'DIAGNOSIS' ? 'var(--accent)' : '' }}>
                    <div className="healing-card-header">
                      <span className="healing-card-title">
                        <Cpu size={14} color="var(--accent)" /> AI Diagnosis
                      </span>
                      <span className={`severity-pill ${incidents[0].diagnosis_source === 'OLLAMA' || incidents[0].ai_mode?.includes('OLLAMA') ? 'medium' : 'low'}`} style={{ fontSize: '9px', padding: '2px 6px', fontWeight: 700 }}>
                        {incidents[0].diagnosis_source || (incidents[0].ai_mode?.includes('OLLAMA') ? 'OLLAMA (llama3.2)' : 'RULE_BASED_ENGINE')}
                      </span>
                    </div>
                    <div className="healing-card-body">
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><Eye size={10} /> Observed Failure</span>
                        <span className="healing-metric-value">{incidents[0].observed}</span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><FileCode2 size={10} /> Evidence Collected</span>
                        <span className="healing-metric-value mono">{incidents[0].evidence}</span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><Cpu size={10} /> Diagnostic Hypothesis</span>
                        <span className="healing-metric-value" style={{ color: 'var(--accent)' }}>{incidents[0].hypothesis}</span>
                      </div>
                      <div className="healing-metric-row" style={{ flexDirection: 'row', justifyContent: 'space-between' }}>
                        <div>
                          <span className="healing-metric-label"><Activity size={10} /> Confidence</span>
                          <span className="healing-metric-value mono" style={{ color: 'var(--healthy)', fontWeight: 700 }}>{incidents[0].confidence}</span>
                        </div>
                        <div>
                          <span className="healing-metric-label"><ShieldAlert size={10} /> Blast Radius</span>
                          <span className="healing-metric-value mono">{incidents[0].blast_radius}</span>
                        </div>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><GitBranch size={10} /> Recommended Action</span>
                        <span className="healing-metric-value mono" style={{ fontWeight: 700 }}>{incidents[0].action}</span>
                      </div>
                    </div>
                  </div>

                  {/* CARD 2: POLICY GATE */}
                  <div className={`healing-card ${selectedHealingStep === 'POLICY_GATE' ? 'selected-healing-step' : ''}`} style={{ borderColor: selectedHealingStep === 'POLICY_GATE' ? 'var(--accent)' : '' }}>
                    <div className="healing-card-header">
                      <span className="healing-card-title">
                        <ShieldCheck size={14} color="var(--healthy)" /> Policy Gate Safety
                      </span>
                      <span className={`risk-pill ${incidents[0].action === 'AUTO_FIX' ? 'low' : 'high'}`}>
                        {incidents[0].action === 'AUTO_FIX' ? 'LOW RISK' : 'HIGH RISK'}
                      </span>
                    </div>
                    <div className="healing-card-body">
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><ShieldAlert size={10} /> Risk Assessment</span>
                        <span className="healing-metric-value">{incidents[0].action === 'AUTO_FIX' ? 'LOW (Idempotent dataset repair)' : 'HIGH (Destructive DDL / contract change)'}</span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><BarChart2 size={10} /> Confidence Threshold Check</span>
                        <span className="healing-metric-value mono" style={{ color: 'var(--healthy)' }}>
                          Threshold: 0.85 (85%) | Score: {incidents[0].confidence} (PASSED)
                        </span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><RefreshCw size={10} /> Idempotency Check</span>
                        <span className="healing-metric-value mono">
                          {incidents[0].action === 'AUTO_FIX' ? 'YES (Safe to execute repeatedly)' : 'NO (Manual review required)'}
                        </span>
                      </div>
                      <div className={`policy-decision-banner ${incidents[0].action === 'AUTO_FIX' ? 'auto-fix' : 'escalate'}`}>
                        <span>Policy Decision:</span>
                        <span className="mono">{incidents[0].action}</span>
                      </div>
                    </div>
                  </div>

                  {/* CARD 3: VERIFICATION */}
                  <div className={`healing-card ${selectedHealingStep === 'VERIFICATION' ? 'selected-healing-step' : ''}`} style={{ borderColor: selectedHealingStep === 'VERIFICATION' ? 'var(--accent)' : '' }}>
                    <div className="healing-card-header">
                      <span className="healing-card-title">
                        <CheckCircle2 size={14} color="var(--healthy)" /> Verification & Recovery
                      </span>
                      <span className="rem-history-badge" style={{ margin: 0 }}>
                        {incidents[0].status.toUpperCase()}
                      </span>
                    </div>
                    <div className="healing-card-body">
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><AlertCircle size={10} /> Before Remediation Value</span>
                        <span className="healing-metric-value mono" style={{ color: '#ef4444' }}>{incidents[0].observed}</span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><CheckCircle2 size={10} /> After Remediation Value</span>
                        <span className="healing-metric-value mono" style={{ color: 'var(--healthy)' }}>
                          {incidents[0].status === 'remediated' || incidents[0].verification_status === 'PASSED' ? '300 clean records (0 duplicates)' : 'Pending Verification Execution'}
                        </span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><ShieldCheck size={10} /> Verification Assertion</span>
                        <span className="healing-metric-value mono" style={{ color: 'var(--healthy)', fontWeight: 700 }}>
                          {incidents[0].verification_status || (incidents[0].status === 'remediated' ? 'PASSED (Zero duplicate primary keys found)' : 'PENDING')}
                        </span>
                      </div>
                      <div className="healing-metric-row">
                        <span className="healing-metric-label"><Activity size={10} /> Final Incident Status</span>
                        <span className="healing-metric-value" style={{ color: incidents[0].status === 'remediated' ? 'var(--healthy)' : '#f59e0b', fontWeight: 700 }}>
                          {incidents[0].status === 'remediated' ? 'RESOLVED (Pipeline Recovered)' : incidents[0].status.toUpperCase()}
                        </span>
                      </div>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        {/* VIEW MODE 2: Interactive Airflow DAG Canvas */}
        {viewMode === 'canvas' && (
          <div className="canvas-workspace">
            <div className="pipeline-flow">
              <svg className="pipeline-svg-connections">
                {connections.map((conn, idx) => (
                  <path 
                    key={idx}
                    d={conn.path}
                    className={`pipeline-connection-path ${conn.status}`}
                  />
                ))}
              </svg>
              
              <div className="flow-column">
                <div className="flow-link-label" style={{ top: '-14px' }}>Data Sources</div>
                
                <div 
                  data-node-id="source_customers"
                  className={`node-card info ${activeNode === 'source_customers' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('source_customers')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Database size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px', color: '#94a3b8', backgroundColor: 'transparent' }}>
                      {nodeDurations['source_customers']}
                    </span>
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
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px', color: '#94a3b8', backgroundColor: 'transparent' }}>
                      {nodeDurations['source_products']}
                    </span>
                  </div>
                  <h3 className="node-title">products.csv</h3>
                  <p className="node-subtitle">Staged Dimensions</p>
                </div>
              </div>

              <div className="flow-column">
                <div className="flow-link-label" style={{ top: '-14px' }}>INGESTION</div>

                <div 
                  data-node-id="ingest_dimensions"
                  className={`node-card ${nodeStatuses['ingest_dimensions'] || 'idle'} ${activeNode === 'ingest_dimensions' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('ingest_dimensions')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Server size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['ingest_dimensions']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['ingest_dimensions']})
                    </span>
                  </div>
                  <h3 className="node-title">Ingest Dimensions</h3>
                  <p className="node-subtitle">Task Group: ingestion</p>
                </div>

                <div 
                  data-node-id="ingest_orders"
                  className={`node-card ${nodeStatuses['ingest_orders'] || 'idle'} ${activeNode === 'ingest_orders' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('ingest_orders')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Server size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['ingest_orders']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['ingest_orders']})
                    </span>
                  </div>
                  <h3 className="node-title">Ingest Orders</h3>
                  <p className="node-subtitle">Task Group: ingestion</p>
                </div>

                <div 
                  data-node-id="ingest_events"
                  className={`node-card ${nodeStatuses['ingest_events'] || 'idle'} ${activeNode === 'ingest_events' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('ingest_events')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Server size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['ingest_events']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['ingest_events']})
                    </span>
                  </div>
                  <h3 className="node-title">Ingest Events</h3>
                  <p className="node-subtitle">Task Group: ingestion</p>
                </div>
              </div>

              <div className="flow-column">
                <div className="flow-link-label" style={{ top: '-14px' }}>VALIDATION</div>

                <div 
                  data-node-id="validate_schema"
                  className={`node-card ${nodeStatuses['validate_schema'] || 'idle'} ${activeNode === 'validate_schema' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('validate_schema')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><FileCode2 size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['validate_schema']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['validate_schema']})
                    </span>
                  </div>
                  <h3 className="node-title">Validate Schema</h3>
                  <p className="node-subtitle">Task Group: validation</p>
                </div>

                <div 
                  data-node-id="validate_quality"
                  className={`node-card ${nodeStatuses['validate_quality'] || 'idle'} ${activeNode === 'validate_quality' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('validate_quality')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><AlertTriangle size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['validate_quality']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['validate_quality']})
                    </span>
                  </div>
                  <h3 className="node-title">Validate Quality</h3>
                  <p className="node-subtitle">Task Group: validation</p>
                </div>
              </div>

              <div className="flow-column">
                <div className="flow-link-label" style={{ top: '-14px' }}>TRANSFORM</div>

                <div 
                  data-node-id="transform_data"
                  className={`node-card ${nodeStatuses['transform_data'] || 'idle'} ${activeNode === 'transform_data' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('transform_data')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Layers size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['transform_data']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['transform_data']})
                    </span>
                  </div>
                  <h3 className="node-title">Transform Data</h3>
                  <p className="node-subtitle">Task Group: transformation</p>
                </div>
              </div>

              <div className="flow-column">
                <div className="flow-link-label" style={{ top: '-14px' }}>LOAD</div>

                <div 
                  data-node-id="load_data"
                  className={`node-card ${nodeStatuses['load_data'] || 'idle'} ${activeNode === 'load_data' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('load_data')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Database size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['load_data']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['load_data']})
                    </span>
                  </div>
                  <h3 className="node-title">Load to Storage / BQ</h3>
                  <p className="node-subtitle">Task Group: load</p>
                </div>
              </div>

              <div className="flow-column">
                <div className="flow-link-label" style={{ top: '-14px' }}>MONITORING</div>

                <div 
                  data-node-id="agent_monitoring"
                  className={`node-card ${nodeStatuses['agent_monitoring'] || 'idle'} ${activeNode === 'agent_monitoring' ? 'selected' : ''}`}
                  onClick={() => setActiveNode('agent_monitoring')}
                >
                  <div className="node-header">
                    <div className="node-icon-wrapper"><Activity size={14} /></div>
                    <span className="rem-history-badge" style={{ margin: 0, fontSize: '8px' }}>
                      {nodeStatuses['agent_monitoring']?.toUpperCase() || 'SUCCESS'} ({nodeDurations['agent_monitoring']})
                    </span>
                  </div>
                  <h3 className="node-title">Agent Monitoring</h3>
                  <p className="node-subtitle">Task Group: monitoring</p>
                </div>
              </div>

            </div>
          </div>
        )}

        {/* Right slide-over Inspector (Item 9) */}
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
              {activeIncident ? (
                <div className={`inspector-incident-card ${activeIncident.severity === 'medium' ? 'warning' : ''}`}>
                  <div className="incident-badge-row">
                    <span style={{ fontWeight: 700, fontSize: '12px', display: 'flex', alignItems: 'center', gap: '6px', color: activeIncident.severity === 'high' ? '#ef4444' : '#d97706' }}>
                      <AlertTriangle size={14} />
                      Incident {activeIncident.id}
                    </span>
                    <span className={`severity-pill ${activeIncident.severity}`}>
                      {activeIncident.severity.toUpperCase()}
                    </span>
                  </div>

                  <div className="diagnostic-stepper">
                    <div className="diagnostic-step">
                      <span className="step-label"><Eye size={11} /> OBSERVED:</span>
                      <span className="step-value">{activeIncident.observed}</span>
                    </div>
                    <div className="diagnostic-step">
                      <span className="step-label"><Check size={11} /> EXPECTED:</span>
                      <span className="step-value">{activeIncident.expected}</span>
                    </div>
                    <div className="diagnostic-step">
                      <span className="step-label"><FileCode2 size={11} /> EVIDENCE:</span>
                      <span className="step-value">{activeIncident.evidence}</span>
                    </div>
                    <div className="diagnostic-step">
                      <span className="step-label"><Cpu size={11} /> DIAGNOSIS:</span>
                      <span className="step-value">{activeIncident.hypothesis}</span>
                    </div>
                    {activeIncident.possible_root_causes && (
                      <div className="diagnostic-step">
                        <span className="step-label"><Layers size={11} /> ROOT CAUSES:</span>
                        <span className="step-value">{activeIncident.possible_root_causes.join("; ")}</span>
                      </div>
                    )}
                    <div className="diagnostic-step">
                      <span className="step-label"><Activity size={11} /> CONFIDENCE:</span>
                      <span className="step-value" style={{ color: 'var(--healthy)', fontWeight: 700 }}>{activeIncident.confidence}</span>
                    </div>
                    <div className="diagnostic-step">
                      <span className="step-label"><ShieldAlert size={11} /> BLAST RADIUS:</span>
                      <span className="step-value">{activeIncident.blast_radius}</span>
                    </div>
                    <div className="diagnostic-step">
                      <span className="step-label"><GitBranch size={11} /> ACTION:</span>
                      <span className="step-value" style={{ color: activeIncident.action === 'AUTO_FIX' ? 'var(--healthy)' : 'var(--warning)', fontWeight: 700 }}>
                        {activeIncident.action}
                      </span>
                    </div>
                  </div>

                  <div className="policy-gate-box">
                    <div className="policy-gate-title">
                      <GitBranch size={13} color="var(--accent)" /> Policy Decision & Verification Flow
                    </div>
                    {activeIncident.action === 'AUTO_FIX' ? (
                      <div className="policy-branch-flow">
                        <span className="policy-badge auto-fix">AUTO_FIX</span>
                        <span>→ Diagnosis</span>
                        <span>→ Policy Gate</span>
                        <span>→ Remediation</span>
                        <span>→ Verification</span>
                        <span style={{ color: 'var(--healthy)', fontWeight: 700 }}>→ RESOLVED</span>
                      </div>
                    ) : (
                      <div className="policy-branch-flow">
                        <span className="policy-badge escalate">ESCALATE</span>
                        <span>→ Diagnosis</span>
                        <span>→ Policy Gate</span>
                        <span>→ Human Review</span>
                        <span style={{ color: 'var(--warning)', fontWeight: 700 }}>→ PENDING</span>
                      </div>
                    )}
                  </div>

                  <div className="action-buttons-group">
                    {activeIncident.action === 'AUTO_FIX' ? (
                      <button 
                        className="btn btn-action-primary"
                        onClick={() => handleApproveRemediation(activeIncident.id)}
                        disabled={isProcessing === activeIncident.id}
                      >
                        {isProcessing === activeIncident.id ? (
                          <>
                            <RefreshCw size={12} className="animate-spin" />
                            Executing & Verifying...
                          </>
                        ) : (
                          <>
                            <UserCheck size={12} />
                            Execute & Verify Auto-Fix
                          </>
                        )}
                      </button>
                    ) : (
                      <button 
                        className="btn btn-action-primary"
                        style={{ backgroundColor: 'var(--warning)', color: '#000000' }}
                        onClick={() => handleDeclineIncident(activeIncident.id)}
                      >
                        <ShieldAlert size={12} />
                        Escalate to Human
                      </button>
                    )}
                  </div>
                </div>
              ) : null}

              <div className="inspector-section">
                <h3 className="inspector-section-title">Task Specification</h3>
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
                  {selectedNodeInfo.details.description && (
                    <div className="property-row">
                      <span className="property-label">Function</span>
                      <span className="property-value">{selectedNodeInfo.details.description}</span>
                    </div>
                  )}
                </div>
              </div>

              {selectedNodeInfo.details.schema && (
                <div className="inspector-section">
                  <h3 className="inspector-section-title">Schema Definition</h3>
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
            </div>
          </aside>
        )}

        {/* Telemetry Drawer */}
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
                className={`telemetry-tab-btn ${activeTab === 'profiling' ? 'active' : ''}`}
                onClick={() => setActiveTab('profiling')}
              >
                Data Profiling
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
            {activeTab === 'logs' && (
              <div className="terminal-console">
                {consoleLogs.map((log, idx) => (
                  <div className="terminal-line" key={idx}>
                    <span className="terminal-time">[{log.time}]</span>
                    <span className={`terminal-level ${log.level}`}>{log.level.toUpperCase()}</span>
                    <span>{log.text}</span>
                  </div>
                ))}
              </div>
            )}

            {/* Data Profiling Inspector Tab (Item 1) */}
            {activeTab === 'profiling' && (
              <table className="profiling-table">
                <thead>
                  <tr>
                    <th>Dataset</th>
                    <th>Row Count</th>
                    <th>Null %</th>
                    <th>Duplicate %</th>
                    <th>Freshness SLA</th>
                  </tr>
                </thead>
                <tbody>
                  {profiles.map((p, idx) => (
                    <tr key={idx}>
                      <td style={{ fontWeight: 700 }}>{p.dataset}</td>
                      <td>{p.row_count}</td>
                      <td style={{ color: p.null_pct > 2 ? '#ef4444' : 'var(--healthy)' }}>{p.null_pct}%</td>
                      <td style={{ color: p.duplicate_pct > 0 ? '#f59e0b' : 'var(--healthy)' }}>{p.duplicate_pct}%</td>
                      <td>{p.freshness_sla}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}

            {activeTab === 'remediations' && (
              <div className="remediation-history-list">
                {remediations.map((rem, idx) => (
                  <div className="rem-history-item" key={idx}>
                    <div className="rem-history-left">
                      <span className="rem-history-title">{rem.fault} Remediation</span>
                      <span className="rem-history-desc">Target Dataset: <code>{rem.target}</code> | Method: {rem.method}</span>
                    </div>
                    <div className="rem-history-right">
                      <span className="rem-history-time">{rem.time}</span>
                      <div>
                        <span className="rem-history-badge">REMEDIATED ({rem.duration})</span>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}

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
                    <h4>Incidents Resolved</h4>
                    <p className="stat-val">{remediations.length}</p>
                  </div>
                  <div className="stat-icon" style={{ color: 'var(--accent)' }}><ShieldCheck size={16} /></div>
                </div>

                <div className="stat-item">
                  <div className="stat-info">
                    <h4>Storage Size</h4>
                    <p className="stat-val">14.8 MB</p>
                  </div>
                  <div className="stat-icon" style={{ color: '#0ea5e9' }}><Database size={16} /></div>
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
