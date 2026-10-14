"""GPO Spend Intelligence  -  maverick-spend review write-back app.

Closes the insight -> action loop: reads the model's High-risk lines from the
Delta gold table (via the SQL warehouse) and lets an analyst flag one for review,
which writes an operational task to Lakebase (Postgres).

Run locally:  uvicorn main:app --reload
Deploy:       databricks apps deploy gpo-spend-intelligence
"""
import os
import json
import time
import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

# Lakebase configuration and functions (inlined from db.py)
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "")
LAKEBASE_BRANCH = os.environ.get("LAKEBASE_BRANCH", "production")
LAKEBASE_ENDPOINT = os.environ.get("LAKEBASE_ENDPOINT", "primary")
PGDATABASE = os.environ.get("PGDATABASE", "databricks_postgres")
LAKEBASE_SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "gpo")

LAKEBASE_DDL = f"""
CREATE SCHEMA IF NOT EXISTS {LAKEBASE_SCHEMA};
CREATE TABLE IF NOT EXISTS {LAKEBASE_SCHEMA}.maverick_review_tasks (
    task_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    line_id      TEXT NOT NULL,
    member_id    TEXT,
    category     TEXT,
    risk_prob    DOUBLE PRECISION,
    status       TEXT NOT NULL DEFAULT 'open',
    assigned_to  TEXT,
    note         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

def _db_conn():
    """Open a Postgres connection to the Lakebase endpoint using an OAuth token."""
    w = WorkspaceClient()
    ep_path = f"projects/{LAKEBASE_PROJECT}/branches/{LAKEBASE_BRANCH}/endpoints/{LAKEBASE_ENDPOINT}"
    host = w.postgres.get_endpoint(ep_path).status.hosts.host
    token = w.postgres.generate_database_credential(ep_path).token
    user = w.current_user.me().user_name
    return psycopg.connect(host=host, dbname=PGDATABASE, user=user,
                           password=token, sslmode="require")

def ensure_schema():
    with _db_conn() as c, c.cursor() as cur:
        cur.execute(LAKEBASE_DDL)
        c.commit()

def flag_for_review(line_id, member_id, category, risk_prob, assigned_to=None, note=None):
    """The app's write: persist an analyst's 'Flag for review' action."""
    with _db_conn() as c, c.cursor() as cur:
        cur.execute(
            f"""INSERT INTO {LAKEBASE_SCHEMA}.maverick_review_tasks
                (line_id, member_id, category, risk_prob, status, assigned_to, note)
                VALUES (%s, %s, %s, %s, 'open', %s, %s) RETURNING task_id""",
            (line_id, member_id, category, risk_prob, assigned_to, note))
        task_id = cur.fetchone()[0]
        c.commit()
        return task_id

def set_status(task_id, status, note=None):
    with _db_conn() as c, c.cursor() as cur:
        cur.execute(
            f"""UPDATE {LAKEBASE_SCHEMA}.maverick_review_tasks
                SET status = %s, note = COALESCE(%s, note), updated_at = now()
                WHERE task_id = %s""",
            (status, note, task_id))
        c.commit()
        return cur.rowcount

def list_tasks(limit=100):
    with _db_conn() as c, c.cursor() as cur:
        cur.execute(
            f"""SELECT task_id, line_id, member_id, category, risk_prob, status, assigned_to, note
                FROM {LAKEBASE_SCHEMA}.maverick_review_tasks ORDER BY task_id LIMIT %s""", (limit,))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]

def status_counts():
    with _db_conn() as c, c.cursor() as cur:
        cur.execute(f"SELECT status, count(*) FROM {LAKEBASE_SCHEMA}.maverick_review_tasks GROUP BY status ORDER BY status")
        return {r[0]: r[1] for r in cur.fetchall()}

app = FastAPI(title="GPO Maverick Review")
CATALOG = os.environ.get("GPO_CATALOG", "main")
SCHEMA = os.environ.get("GPO_SCHEMA", "gpo_spend_intelligence")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "<warehouse_id>")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "<genie_space_id>")
FQ = f"{CATALOG}.{SCHEMA}"


def _query(sql):
    """Execute a SQL query on the warehouse and return results."""
    w = WorkspaceClient()
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql, wait_timeout="30s")
    while r.status.state in (StatementState.PENDING, StatementState.RUNNING):
        r = w.statement_execution.get_statement(r.statement_id)
    if r.status.state != StatementState.SUCCEEDED:
        raise HTTPException(500, f"query failed: {r.status.state}")
    return r.result.data_array or []


class FlagRequest(BaseModel):
    line_id: str
    member_id: str | None = None
    category: str | None = None
    risk_prob: float | None = None
    assigned_to: str | None = None
    note: str | None = None


class StatusRequest(BaseModel):
    task_id: int
    status: str
    note: str | None = None


class GenieAskRequest(BaseModel):
    question: str


@app.on_event("startup")
def _startup():
    try:
        ensure_schema()
    except Exception as e:
        print(f"Warning: Could not ensure Lakebase schema: {e}")


@app.get("/api/kpis")
def kpis():
    """Overview KPIs: spend under mgmt, coverage, off-contract, savings realized."""
    # Real query against the governed views  -  correct column names (values are in $M).
    rows = _query(f"""
        SELECT k.spend_under_mgmt_m, k.contract_coverage_pct, k.maverick_spend_m,
               k.savings_realized_m, e.incremental_gpo_fee_m,
               e.gpo_fee_revenue_today_m, e.gpo_fee_revenue_at_target_m, e.coverage_target_pct
        FROM {FQ}.gold_kpis k CROSS JOIN {FQ}.gold_gpo_economics e
    """)
    r = rows[0]
    return {
        "spend_under_mgmt": float(r[0]) * 1e6,
        "contract_coverage_pct": float(r[1]),
        "off_contract_spend": float(r[2]) * 1e6,
        "savings_realized": float(r[3]) * 1e6,
        "coverage_revenue_impact_annual": float(r[4]) * 1e6,
        "fee_today": float(r[5]) * 1e6,
        "fee_at_target": float(r[6]) * 1e6,
        "coverage_target_pct": float(r[7]),
    }


@app.get("/api/high-risk")
def high_risk(limit: int = 25):
    """Top High-risk maverick lines with action briefs and actionability scores."""
    # Drive from gold_action_briefs so every row carries a real GenAI recommendation + score.
    rows = _query(f"""
        SELECT b.line_id, b.member_id, b.category, b.risk_prob,
               b.action_brief, COALESCE(q.actionability_score, 0)
        FROM {FQ}.gold_action_briefs b
        LEFT JOIN {FQ}.gold_action_brief_quality q ON b.line_id = q.line_id
        ORDER BY b.risk_prob DESC LIMIT {int(limit)}
    """)
    return [
        {
            "line_id": r[0], "member_id": r[1], "category": r[2],
            "risk_prob": float(r[3]), "action_brief": r[4],
            "actionability_score": float(r[5]),
        }
        for r in rows
    ]


@app.post("/api/flag")
def flag(req: FlagRequest):
    """The write: persist a 'Flag for review' action to Lakebase."""
    # Real Lakebase write  -  fail honestly if it can't persist (no fabricated task_id).
    task_id = flag_for_review(req.line_id, req.member_id, req.category,
                              req.risk_prob, req.assigned_to, req.note)
    return {"task_id": task_id, "status": "open"}


@app.post("/api/status")
def status(req: StatusRequest):
    n = set_status(req.task_id, req.status, req.note)
    if not n:
        raise HTTPException(404, "task not found")
    return {"task_id": req.task_id, "status": req.status, "updated": n}


@app.get("/api/tasks")
def tasks(limit: int = 100):
    return {"tasks": list_tasks(limit), "counts": status_counts()}


@app.post("/api/genie/ask")
def genie_ask(req: GenieAskRequest):
    """Call Genie Conversation API to answer the question."""
    # Real Genie Conversation API: start_conversation_and_wait takes (space_id, content)
    # and returns the first GenieMessage. Parse its attachments for text + generated SQL.
    w = WorkspaceClient()
    msg = w.genie.start_conversation_and_wait(GENIE_SPACE_ID, req.question)
    answer, generated_sql, rows = "", "", []
    for att in (msg.attachments or []):
        if getattr(att, "text", None) and att.text.content:
            answer = att.text.content
        if getattr(att, "query", None):
            generated_sql = att.query.query or ""
            if not answer and att.query.description:
                answer = att.query.description
            try:
                res = w.genie.get_message_query_result_by_attachment(
                    GENIE_SPACE_ID, msg.conversation_id, msg.id, att.attachment_id)
                sr = getattr(res, "statement_response", None)
                if sr and sr.result and sr.result.data_array:
                    rows = sr.result.data_array[:10]
            except Exception:
                pass
    return {"answer": answer or "(Genie returned no text answer)",
            "generated_sql": generated_sql, "rows": rows}


@app.get("/api/spend-explorer")
def spend_explorer():
    """Spend by region + members ranked by off-contract $ (Spend Explorer tab)."""
    regions = _query(f"SELECT region, spend_m, maverick_pct, savings_m FROM {FQ}.gold_region_summary ORDER BY maverick_pct DESC")
    members = _query(f"SELECT member_id, region, total_spend_m, off_contract_m, maverick_pct FROM {FQ}.gold_member_maverick ORDER BY off_contract_m DESC LIMIT 10")
    return {
        "regions": [{"region": r[0], "spend_m": float(r[1]), "maverick_pct": float(r[2]), "savings_m": float(r[3])} for r in regions],
        "members": [{"member_id": m[0], "region": m[1], "total_spend_m": float(m[2]), "off_contract_m": float(m[3]), "maverick_pct": float(m[4])} for m in members],
    }


@app.get("/api/contract-intelligence")
def contract_intelligence():
    """Coverage by category + contract expirations (Contract Intelligence tab)."""
    cats = _query(f"SELECT category, spend_m, coverage_pct, savings_m FROM {FQ}.gold_coverage_by_category ORDER BY coverage_pct ASC")
    exps = _query(f"SELECT expiration_status, risk_level, contracts, annual_value_m FROM {FQ}.gold_contract_expirations ORDER BY annual_value_m DESC")
    return {
        "categories": [{"category": c[0], "spend_m": float(c[1]), "coverage_pct": float(c[2]), "savings_m": float(c[3])} for c in cats],
        "expirations": [{"status": e[0], "risk_level": e[1], "contracts": int(e[2]), "annual_value_m": float(e[3])} for e in exps],
    }


@app.get("/", response_class=HTMLResponse)
def home():
    """Single-page HTML UI with KPIs, Maverick Review, and Ask Genie."""
    return """<!doctype html>
<html>
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>GPO Spend Intelligence</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
            background: #ffffff;
            color: #1b3139;
        }
        header {
            background: #1b3139;
            color: white;
            padding: 24px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        header h1 { font-size: 28px; font-weight: 600; }
        header p { font-size: 14px; margin-top: 4px; opacity: 0.9; }

        .container {
            max-width: 1200px;
            margin: 0 auto;
            padding: 24px;
        }

        .section {
            background: #f9f7f4;
            border: 1px solid #e8e3d8;
            border-radius: 8px;
            padding: 24px;
            margin-bottom: 24px;
        }

        .section h2 {
            font-size: 18px;
            font-weight: 600;
            margin-bottom: 16px;
            color: #1b3139;
        }

        .kpi-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 16px;
        }

        .kpi-tile {
            background: white;
            border: 1px solid #d4d0c5;
            border-radius: 6px;
            padding: 16px;
            text-align: center;
        }

        .kpi-tile .label {
            font-size: 12px;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 8px;
        }

        .kpi-tile .value {
            font-size: 24px;
            font-weight: 600;
            color: #1b3139;
        }

        .kpi-tile.highlight {
            background: #fffef9;
            border-color: #c4b5a0;
        }

        .maverick-table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }

        .maverick-table th {
            background: #f0ede7;
            color: #1b3139;
            padding: 12px;
            text-align: left;
            font-weight: 600;
            font-size: 13px;
            border-bottom: 2px solid #d4d0c5;
        }

        .maverick-table td {
            padding: 12px;
            border-bottom: 1px solid #e8e3d8;
            font-size: 13px;
        }

        .maverick-table tr:hover {
            background: #faf9f7;
        }

        .risk-high { color: #d64545; font-weight: 600; }
        .btn-flag {
            background: #1b3139;
            color: white;
            border: none;
            padding: 6px 12px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 12px;
            font-weight: 500;
        }

        .btn-flag:hover { background: #2d4d5c; }
        .btn-flag:disabled { background: #a0a0a0; cursor: not-allowed; }

        .genie-box {
            background: white;
            border: 1px solid #d4d0c5;
            border-radius: 6px;
            padding: 16px;
        }

        .genie-input-group {
            display: flex;
            gap: 8px;
            margin-bottom: 16px;
        }

        .genie-input-group input {
            flex: 1;
            padding: 10px;
            border: 1px solid #d4d0c5;
            border-radius: 4px;
            font-size: 14px;
        }

        .genie-input-group button {
            background: #1b3139;
            color: white;
            border: none;
            padding: 10px 20px;
            border-radius: 4px;
            cursor: pointer;
            font-weight: 500;
        }

        .genie-input-group button:hover { background: #2d4d5c; }
        .genie-input-group button:disabled { background: #a0a0a0; cursor: not-allowed; }

        .genie-response {
            background: #f9f7f4;
            border: 1px solid #e8e3d8;
            border-radius: 4px;
            padding: 12px;
            margin-bottom: 12px;
            font-size: 14px;
            line-height: 1.5;
        }

        .genie-sql {
            background: #f0ede7;
            border-left: 3px solid #1b3139;
            padding: 10px;
            font-family: monospace;
            font-size: 12px;
            overflow-x: auto;
            margin-top: 8px;
            color: #333;
        }

        .sql-toggle {
            color: #1b3139;
            cursor: pointer;
            text-decoration: underline;
            font-size: 12px;
            margin-top: 8px;
        }

        .loading { color: #999; font-style: italic; }
        .error { color: #d64545; }
        .success { color: #2d8659; }

        .status-open { color: #ff9800; font-weight: 600; }
        .status-closed { color: #2d8659; font-weight: 600; }

        .tabnav { display: flex; flex-wrap: wrap; gap: 4px; background: #1b3139; padding: 0 24px; }
        .tabnav button {
            background: transparent; color: #cfd8dc; border: none; border-bottom: 3px solid transparent;
            padding: 12px 16px; cursor: pointer; font-size: 14px; font-weight: 500;
        }
        .tabnav button:hover { color: #fff; }
        .tabnav button.active { color: #fff; border-bottom-color: #ff3621; }
        .tab-panel { display: none; }
        .tab-panel.active { display: block; }

        .revbar { background: #1b3139; color: #fff; border-radius: 6px; padding: 14px 18px; margin-top: 8px; font-size: 15px; }
        .revbar b { color: #ffd9a0; }
        .revbar .note { display:block; color:#cfd8dc; font-size:12px; margin-top:4px; }
        .barcell { position: relative; min-width: 130px; }
        .barcell .bar { display:inline-block; height:12px; background:#d64545; border-radius:3px; vertical-align:middle; margin-right:8px; }
        .barcell .pct { font-weight:600; }
    </style>
</head>
<body>
    <header>
        <h1>GPO Spend Intelligence</h1>
        <p>Score in Delta, act in Lakebase. Intelligent maverick-spend review at scale.</p>
    </header>

    <div class="tabnav">
        <button class="active" data-tab="overview" onclick="showTab('overview')">Overview</button>
        <button data-tab="spend" onclick="showTab('spend')">Spend Explorer</button>
        <button data-tab="contract" onclick="showTab('contract')">Contract Intelligence</button>
        <button data-tab="maverick" onclick="showTab('maverick')">Maverick Review</button>
        <button data-tab="genie" onclick="showTab('genie')">Ask Genie</button>
    </div>

    <div class="container">
        <!-- Overview -->
        <div class="tab-panel active" id="panel-overview">
          <div class="section">
            <h2>Overview KPIs</h2>
            <div id="kpi-container" class="kpi-grid">
                <div class="kpi-tile"><div class="label">Spend Under Mgmt</div><div class="value" id="spend">-</div></div>
                <div class="kpi-tile"><div class="label">Contract Coverage</div><div class="value" id="coverage">-</div></div>
                <div class="kpi-tile"><div class="label">Off-Contract Spend</div><div class="value" id="offcontract">-</div></div>
                <div class="kpi-tile"><div class="label">Savings Realized</div><div class="value" id="savings">-</div></div>
                <div class="kpi-tile highlight"><div class="label">Coverage is Revenue (+Annual GPO fee)</div><div class="value" id="revenue">-</div></div>
            </div>
            <div class="revbar" id="revbar">GPO fee: <b id="feeToday">-</b> today &rarr; <b id="feeTarget">-</b> at <b id="covTarget">85%</b> coverage = <b id="feeDelta">-</b> /yr recurring<span class="note">Illustrative at a 2.5% contract-admin fee. Coverage is the GPO's revenue base, not just an efficiency metric.</span></div>
          </div>
        </div>

        <!-- Spend Explorer -->
        <div class="tab-panel" id="panel-spend">
          <div class="section">
            <h2>Spend Explorer  -  off-contract by region</h2>
            <div style="overflow-x:auto"><table class="maverick-table"><thead><tr><th>Region</th><th>Spend $M</th><th>Maverick %</th><th>Savings $M</th></tr></thead><tbody id="region-rows"><tr><td colspan="4" class="loading">Loading...</td></tr></tbody></table></div>
            <h2 style="margin-top:22px">Top members by off-contract spend</h2>
            <div style="overflow-x:auto"><table class="maverick-table"><thead><tr><th>Member</th><th>Region</th><th>Total $M</th><th>Off-contract $M</th><th>Maverick %</th></tr></thead><tbody id="member-rows"><tr><td colspan="5" class="loading">Loading...</td></tr></tbody></table></div>
          </div>
        </div>

        <!-- Contract Intelligence -->
        <div class="tab-panel" id="panel-contract">
          <div class="section">
            <h2>Contract Intelligence  -  coverage by category</h2>
            <div style="overflow-x:auto"><table class="maverick-table"><thead><tr><th>Category</th><th>Spend $M</th><th>Coverage %</th><th>Savings $M</th></tr></thead><tbody id="category-rows"><tr><td colspan="4" class="loading">Loading...</td></tr></tbody></table></div>
            <h2 style="margin-top:22px">Contract expirations</h2>
            <div style="overflow-x:auto"><table class="maverick-table"><thead><tr><th>Status</th><th>Risk level</th><th>Contracts</th><th>Annual value $M</th></tr></thead><tbody id="expiration-rows"><tr><td colspan="4" class="loading">Loading...</td></tr></tbody></table></div>
          </div>
        </div>

        <!-- Maverick Review -->
        <div class="tab-panel" id="panel-maverick">
          <div class="section">
            <h2>Maverick Review  -  model-ranked High-risk queue</h2>
            <div id="maverick-container" style="overflow-x: auto;">
                <table class="maverick-table">
                    <thead><tr><th>Member</th><th>Category</th><th>Risk %</th><th>Actionability</th><th>AI Brief</th><th>Action</th></tr></thead>
                    <tbody id="maverick-rows"><tr><td colspan="6" class="loading">Loading high-risk lines...</td></tr></tbody>
                </table>
            </div>
          </div>
        </div>

        <!-- Ask Genie -->
        <div class="tab-panel" id="panel-genie">
          <div class="section">
            <h2>Ask Genie</h2>
            <div class="genie-box">
                <div class="genie-input-group">
                    <input type="text" id="genie-input" placeholder="e.g., Which 5 members have the most off-contract spend?" />
                    <button id="genie-submit" onclick="askGenie()">Ask</button>
                </div>
                <div id="genie-result"></div>
            </div>
          </div>
        </div>
    </div>

    <script>
        function formatUSD(v) {
            if (v >= 1_000_000_000) return '$' + (v / 1_000_000_000).toFixed(1) + 'B';
            if (v >= 1_000_000) return '$' + (v / 1_000_000).toFixed(1) + 'M';
            return '$' + v.toLocaleString('en-US', {maximumFractionDigits: 0});
        }

        function formatPct(v) { return v.toFixed(1) + '%'; }

        async function loadKPIs() {
            try {
                const res = await fetch('/api/kpis');
                const data = await res.json();
                document.getElementById('spend').textContent = formatUSD(data.spend_under_mgmt);
                document.getElementById('coverage').textContent = formatPct(data.contract_coverage_pct);
                document.getElementById('offcontract').textContent = formatUSD(data.off_contract_spend);
                document.getElementById('savings').textContent = formatUSD(data.savings_realized);
                document.getElementById('revenue').textContent = '+' + formatUSD(data.coverage_revenue_impact_annual) + '/yr';
                if (data.fee_today) {
                    document.getElementById('feeToday').textContent = formatUSD(data.fee_today);
                    document.getElementById('feeTarget').textContent = formatUSD(data.fee_at_target);
                    document.getElementById('feeDelta').textContent = '+' + formatUSD(data.coverage_revenue_impact_annual);
                    document.getElementById('covTarget').textContent = formatPct(data.coverage_target_pct);
                }
            } catch (e) {
                console.error('KPI load failed:', e);
            }
        }

        async function loadHighRisk() {
            try {
                const res = await fetch('/api/high-risk?limit=25');
                const lines = await res.json();
                const tbody = document.getElementById('maverick-rows');
                tbody.innerHTML = '';
                if (lines.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" class="loading">No high-risk lines found.</td></tr>';
                    return;
                }
                lines.forEach(line => {
                    const tr = document.createElement('tr');
                    tr.innerHTML = `
                        <td>${line.member_id || 'N/A'}</td>
                        <td>${line.category || 'N/A'}</td>
                        <td class="risk-high">${formatPct(line.risk_prob)}</td>
                        <td>${(line.actionability_score || 0).toFixed(2)}</td>
                        <td style="font-size: 12px; max-width: 300px; word-wrap: break-word;">${line.action_brief || 'Review needed'}</td>
                        <td>
                            <button class="btn-flag" onclick="flagLine('${line.line_id}', '${line.member_id}', '${line.category}', ${line.risk_prob})">
                                Flag
                            </button>
                        </td>
                    `;
                    tbody.appendChild(tr);
                });
            } catch (e) {
                console.error('High-risk load failed:', e);
                document.getElementById('maverick-rows').innerHTML = '<tr><td colspan="6" class="error">Failed to load high-risk lines.</td></tr>';
            }
        }

        async function flagLine(lineId, memberId, category, riskProb) {
            if (!confirm('Flag ' + lineId + ' for review?')) return;
            try {
                const res = await fetch('/api/flag', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({line_id: lineId, member_id: memberId, category: category, risk_prob: riskProb})
                });
                const data = await res.json();
                alert('Task #' + data.task_id + ' created (status: ' + data.status + ')');
            } catch (e) {
                alert('Error: ' + e);
            }
        }

        async function askGenie() {
            const input = document.getElementById('genie-input');
            const question = input.value.trim();
            if (!question) { alert('Please enter a question'); return; }

            const btn = document.getElementById('genie-submit');
            btn.disabled = true;
            const result = document.getElementById('genie-result');
            result.innerHTML = '<div class="loading">Asking Genie...</div>';

            try {
                const res = await fetch('/api/genie/ask', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({question: question})
                });
                const data = await res.json();

                let html = '<div class="genie-response"><strong>Answer:</strong><br>' + data.answer + '</div>';
                if (data.generated_sql) {
                    html += '<div class="sql-toggle" onclick="toggleSQL(this)">View SQL</div>';
                    html += '<div class="genie-sql" style="display:none;">' + data.generated_sql + '</div>';
                }
                result.innerHTML = html;
                input.value = '';
            } catch (e) {
                result.innerHTML = '<div class="error">Error: ' + e + '</div>';
            }
            btn.disabled = false;
        }

        function toggleSQL(el) {
            const sql = el.nextElementSibling;
            if (sql && sql.classList.contains('genie-sql')) {
                sql.style.display = sql.style.display === 'none' ? 'block' : 'none';
                el.textContent = sql.style.display === 'none' ? 'View SQL' : 'Hide SQL';
            }
        }

        document.addEventListener('DOMContentLoaded', () => {
            document.getElementById('genie-input').addEventListener('keypress', (e) => {
                if (e.key === 'Enter') askGenie();
            });
        });

        var loaded = {};
        function showTab(name) {
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
            document.getElementById('panel-' + name).classList.add('active');
            document.querySelectorAll('.tabnav button').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
            if (name === 'spend' && !loaded.spend) { loadSpendExplorer(); loaded.spend = true; }
            if (name === 'contract' && !loaded.contract) { loadContractIntel(); loaded.contract = true; }
            if (name === 'maverick' && !loaded.maverick) { loadHighRisk(); loaded.maverick = true; }
        }

        async function loadSpendExplorer() {
            try {
                const d = await (await fetch('/api/spend-explorer')).json();
                const maxR = Math.max(...d.regions.map(r => r.maverick_pct), 1);
                document.getElementById('region-rows').innerHTML = d.regions.map(r =>
                    `<tr><td>${r.region}</td><td>$${r.spend_m.toFixed(1)}M</td><td class="barcell"><span class="bar" style="width:${(r.maverick_pct/maxR*90).toFixed(0)}px"></span><span class="pct risk-high">${r.maverick_pct.toFixed(1)}%</span></td><td>$${r.savings_m.toFixed(1)}M</td></tr>`).join('');
                const maxM = Math.max(...d.members.map(m => m.maverick_pct), 1);
                document.getElementById('member-rows').innerHTML = d.members.map(m =>
                    `<tr><td>${m.member_id}</td><td>${m.region}</td><td>$${m.total_spend_m.toFixed(1)}M</td><td>$${m.off_contract_m.toFixed(1)}M</td><td class="barcell"><span class="bar" style="width:${(m.maverick_pct/maxM*90).toFixed(0)}px"></span><span class="pct risk-high">${m.maverick_pct.toFixed(1)}%</span></td></tr>`).join('');
            } catch (e) { document.getElementById('region-rows').innerHTML = '<tr><td colspan="4" class="error">Failed to load.</td></tr>'; }
        }

        async function loadContractIntel() {
            try {
                const d = await (await fetch('/api/contract-intelligence')).json();
                document.getElementById('category-rows').innerHTML = d.categories.map(c =>
                    `<tr><td>${c.category}</td><td>$${c.spend_m.toFixed(1)}M</td><td>${c.coverage_pct.toFixed(1)}%</td><td>$${c.savings_m.toFixed(1)}M</td></tr>`).join('');
                document.getElementById('expiration-rows').innerHTML = d.expirations.map(e =>
                    `<tr><td>${e.status}</td><td>${e.risk_level}</td><td>${e.contracts}</td><td>$${e.annual_value_m.toFixed(1)}M</td></tr>`).join('');
            } catch (e) { document.getElementById('category-rows').innerHTML = '<tr><td colspan="4" class="error">Failed to load.</td></tr>'; }
        }

        window.addEventListener('load', () => {
            loadKPIs();
            loadHighRisk(); loaded.maverick = true;
        });
    </script>
</body>
</html>"""
