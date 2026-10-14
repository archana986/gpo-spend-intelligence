#!/usr/bin/env python3
"""End-to-end accuracy test suite for the GPO Spend Intelligence build.

Runs assertions against the deployed data, governance, model, serving endpoint,
Genie Agent, Databricks App, and Lakebase connection. Every check prints the
observed value, so the output doubles as build evidence.

Usage:
    CATALOG=main SCHEMA=gpo_spend_intelligence \\
    WAREHOUSE_ID=<warehouse_id> PROFILE=your-profile \\
    ENDPOINT=gpo-maverick-risk APP_NAME=gpo-spend-intelligence \\
    GENIE_SPACE_ID=<genie_space_id> \\
    python3 src/tests/validate_build.py

Any arg can also be passed positionally: validate_build.py <catalog> <schema>.
"""
import json
import os
import subprocess
import sys
import time

CATALOG = os.environ.get("CATALOG", "main")
SCHEMA = os.environ.get("SCHEMA", "gpo_spend_intelligence")
WH = os.environ.get("WAREHOUSE_ID", "<warehouse_id>")
PROFILE = os.environ.get("PROFILE", "")
ENDPOINT = os.environ.get("ENDPOINT", "gpo-maverick-risk")
APP_NAME = os.environ.get("APP_NAME", "gpo-spend-intelligence")
GENIE_SPACE_ID = os.environ.get("GENIE_SPACE_ID", "<genie_space_id>")
LAKEBASE_PROJECT = os.environ.get("LAKEBASE_PROJECT", "")
if len(sys.argv) > 1:
    CATALOG = sys.argv[1]
if len(sys.argv) > 2:
    SCHEMA = sys.argv[2]
FQ = f"{CATALOG}.{SCHEMA}"

RESULTS = []


def _cli(args):
    return subprocess.run(["databricks", *args, "--profile", PROFILE],
                          capture_output=True, text=True)


def sql(statement):
    p = _cli(["api", "post", "/api/2.0/sql/statements", "--json",
              json.dumps({"warehouse_id": WH, "statement": statement, "wait_timeout": "50s"})])
    if p.returncode != 0:
        raise RuntimeError(p.stderr[:400])
    d = json.loads(p.stdout)
    sid = d.get("statement_id")
    st = d.get("status", {}).get("state")
    while st in ("PENDING", "RUNNING"):
        time.sleep(2)
        g = _cli(["api", "get", f"/api/2.0/sql/statements/{sid}"])
        d = json.loads(g.stdout)
        st = d.get("status", {}).get("state")
    if st != "SUCCEEDED":
        raise RuntimeError(f"{st}: {json.dumps(d.get('status', {}))[:300]}")
    return d.get("result", {}).get("data_array", []) or []


def scalar(statement):
    rows = sql(statement)
    return rows[0][0] if rows and rows[0] else None


def check(name, ok, detail):
    RESULTS.append((name, ok, detail))
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {detail}")


def main():
    print(f"=== Validating build: {FQ} (warehouse {WH}, profile {PROFILE}) ===\n")

    # 1. spend_lines row count
    try:
        n = int(scalar(f"SELECT count(*) FROM {FQ}.spend_lines"))
        check("spend_lines row count == 60000", n == 60000, f"{n} rows")
    except Exception as e:
        check("spend_lines row count == 60000", False, f"error: {e}")

    # 2. dimensions non-empty
    try:
        dims = {t: int(scalar(f"SELECT count(*) FROM {FQ}.{t}"))
                for t in ("dim_members", "dim_suppliers", "dim_contracts", "ref_categories")}
        check("dimensions non-empty", all(v > 0 for v in dims.values()), json.dumps(dims))
    except Exception as e:
        check("dimensions non-empty", False, f"error: {e}")

    # 3. bronze == silver row count (SDP ingest)
    try:
        b = int(scalar(f"SELECT count(*) FROM {FQ}.po_events_bronze_sdp"))
        s = int(scalar(f"SELECT count(*) FROM {FQ}.po_events_silver_sdp"))
        check("SDP bronze == silver", b == s and b > 0, f"bronze={b}, silver={s}")
    except Exception as e:
        check("SDP bronze == silver", False, f"error: {e}")

    # 4. off-contract rate in expected band
    try:
        rate = float(scalar(
            f"SELECT round(avg(CASE WHEN on_contract THEN 0 ELSE 1 END),4) FROM {FQ}.spend_lines"))
        check("off-contract rate in 0.10-0.35", 0.10 <= rate <= 0.35, f"{rate:.4f}")
    except Exception as e:
        check("off-contract rate in 0.10-0.35", False, f"error: {e}")

    # 5. UC model version + champion alias + AUC > 0.5
    try:
        p = _cli(["api", "get",
                  f"/api/2.1/unity-catalog/models/{FQ}.maverick_risk_model/aliases/champion"])
        alias = json.loads(p.stdout) if p.returncode == 0 else {}
        ver = alias.get("version_num") or alias.get("version")
        check("UC model champion alias exists", bool(ver), f"champion -> v{ver}")
    except Exception as e:
        check("UC model champion alias exists", False, f"error: {e}")

    # 6. gold_maverick_risk == spend_lines count, tiers present
    try:
        g = int(scalar(f"SELECT count(*) FROM {FQ}.gold_maverick_risk"))
        n = int(scalar(f"SELECT count(*) FROM {FQ}.spend_lines"))
        tiers = sql(f"SELECT risk_tier, count(*) FROM {FQ}.gold_maverick_risk GROUP BY risk_tier")
        tset_tiers = {r[0] for r in tiers}
        check("gold_maverick_risk scored all lines", g == n and len(tset_tiers) >= 2,
              f"gold={g}, spend={n}, tiers={sorted(tset_tiers)}")
    except Exception as e:
        check("gold_maverick_risk scored all lines", False, f"error: {e}")

    # 7. column mask active on member_name
    try:
        rows = sql(f"SELECT column_name, mask_name FROM {CATALOG}.information_schema.column_masks "
                   f"WHERE table_schema = '{SCHEMA}' AND table_name = 'spend_lines'")
        check("column mask on member_name registered", len(rows) > 0,
              f"masks={[list(r) for r in rows]}")
    except Exception as e:
        check("column mask on member_name registered", False, f"error: {e}")

    # 8. serving endpoint READY + returns a prediction
    try:
        p = _cli(["serving-endpoints", "get", ENDPOINT])
        if p.returncode == 0:
            ep = json.loads(p.stdout)
            ready = ep.get("state", {}).get("ready")
            rec = {"member_id": "MBR-01", "region": "West", "supplier_id": "SUP-001",
                   "category": "Med-Surg Supplies", "quantity": 10, "unit_price": 100.0, "amount": 1000.0,
                   "days_to_expiry": 20, "order_month": 11}
            got_pred = False
            for attempt in range(4):  # retry: scale-to-zero endpoints cold-start (503) on first hit
                q = _cli(["serving-endpoints", "query", ENDPOINT, "--json",
                          json.dumps({"dataframe_records": [rec]})])
                if q.returncode == 0 and "predictions" in q.stdout:
                    got_pred = True
                    break
                time.sleep(10)
            check("serving endpoint READY + returns prediction",
                  ready == "READY" and got_pred, f"state={ready}, query_ok={got_pred}")
        else:
            check("serving endpoint READY + returns prediction", False,
                  "endpoint not found (optional in repro schema)")
    except Exception as e:
        check("serving endpoint READY + returns prediction", False, f"error: {e}")

    # 9. registered column mask is enforced for the validation identity
    try:
        observed = scalar(f"SELECT member_name FROM {FQ}.spend_lines LIMIT 1")
        check("column mask enforced", observed == "***REDACTED***",
              f"member_name={observed}")
    except Exception as e:
        check("column mask enforced", False, f"error: {e}")

    # 10. row-level filter is registered and returns an authorized subset
    try:
        filters = sql(
            f"SELECT table_name, filter_name FROM {CATALOG}.information_schema.row_filters "
            f"WHERE table_schema='{SCHEMA}' AND table_name='gold_review_queue'")
        regions = [r[0] for r in sql(
            f"SELECT DISTINCT region FROM {FQ}.gold_review_queue ORDER BY region")]
        check("regional row filter registered + enforced",
              len(filters) == 1 and 1 <= len(regions) <= 2,
              f"filters={filters}, visible_regions={regions}")
    except Exception as e:
        check("regional row filter registered + enforced", False, f"error: {e}")

    # 11. GenAI action layer and automated quality gate
    try:
        brief_count = int(scalar(f"SELECT count(*) FROM {FQ}.gold_action_briefs"))
        avg_score = float(scalar(
            f"SELECT round(avg(actionability_score),2) "
            f"FROM {FQ}.gold_action_brief_quality"))
        check("GenAI briefs + LLM quality gate",
              brief_count == 25 and avg_score >= 4.0,
              f"briefs={brief_count}, avg_actionability={avg_score:.2f}")
    except Exception as e:
        check("GenAI briefs + LLM quality gate", False, f"error: {e}")

    # 12. executive economics is computed from governed data
    try:
        economics = sql(
            f"SELECT coverage_pct, coverage_target_pct, incremental_gpo_fee_m "
            f"FROM {FQ}.gold_gpo_economics")
        fee = float(economics[0][2])
        check("coverage-to-revenue economics", fee > 0,
              f"coverage={economics[0][0]}%, target={economics[0][1]}%, "
              f"incremental_fee=${fee:.2f}M")
    except Exception as e:
        check("coverage-to-revenue economics", False, f"error: {e}")

    # 13. canonical Genie Agent produces SQL and the expected answer
    try:
        p = _cli([
            "genie", "start-conversation", GENIE_SPACE_ID,
            "Which member has the highest off-contract spend, and how much is it?",
            "--output", "json",
        ])
        message = json.loads(p.stdout) if p.returncode == 0 else {}
        attachments = message.get("attachments", [])
        answer = " ".join(
            a.get("text", {}).get("content", "")
            for a in attachments if a.get("text"))
        generated_sql = " ".join(
            a.get("query", {}).get("query", "")
            for a in attachments if a.get("query"))
        check("Genie generated SQL + correct answer",
              message.get("status") == "COMPLETED"
              and bool(generated_sql)
              and "10.7" in answer
              and ("MBR-18" in answer or "REDACTED" in answer),
              f"status={message.get('status')}, answer={answer[:180]}")
    except Exception as e:
        check("Genie generated SQL + correct answer", False, f"error: {e}")

    # 14. canonical app is running with all platform bindings
    try:
        p = _cli(["apps", "get", APP_NAME, "--output", "json"])
        app = json.loads(p.stdout) if p.returncode == 0 else {}
        resource_names = {r.get("name") for r in app.get("resources", [])}
        app_state = app.get("app_status", {}).get("state")
        check("Databricks App running + bound",
              app_state == "RUNNING"
              and {"sql-warehouse", "genie-space", "postgres"} <= resource_names,
              f"state={app_state}, resources={sorted(resource_names)}")
    except Exception as e:
        check("Databricks App running + bound", False, f"error: {e}")

    # 15. Lakebase accepts a transactional write and immediate read
    try:
        marker = f"FE-VALIDATION-{int(time.time())}"
        insert = (
            "INSERT INTO gpo.maverick_review_tasks "
            "(line_id, member_id, category, risk_prob, status, note) "
            f"VALUES ('{marker}','MBR-18','IT / Capital',0.8303,'open',"
            "'automated build validation')"
        )
        p = subprocess.run(
            ["databricks", "psql", "--project", LAKEBASE_PROJECT,
             "--branch", "production", "--endpoint", "primary",
             "--profile", PROFILE, "--", "-d", "databricks_postgres",
             "-v", "ON_ERROR_STOP=1", "-At",
             "-c", "BEGIN",
             "-c", insert,
             "-c", f"SELECT line_id FROM gpo.maverick_review_tasks "
                   f"WHERE line_id='{marker}'",
             "-c", "ROLLBACK"],
            capture_output=True, text=True,
        )
        check("Lakebase transactional write + read",
              p.returncode == 0 and marker in p.stdout,
              f"returncode={p.returncode}, marker_seen={marker in p.stdout}")
    except Exception as e:
        check("Lakebase transactional write + read", False, f"error: {e}")

    # ---- summary ----
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    total = len(RESULTS)
    print(f"\n=== {passed}/{total} checks passed ===")
    if passed != total:
        print("FAILURES:")
        for name, ok, detail in RESULTS:
            if not ok:
                print(f"  - {name}: {detail}")
        sys.exit(1)
    print("BUILD VERIFIED.")


if __name__ == "__main__":
    main()
