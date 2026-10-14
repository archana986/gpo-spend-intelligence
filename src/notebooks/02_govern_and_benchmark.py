# Databricks notebook source
#
# # 02 · Unity Catalog governance + Genie benchmarks
# Applies a column mask on `member_name`, grants schema read, verifies the mask
# is active, and runs the Genie benchmark questions (NL intent -> SQL -> answer)
# over the governed tables. Parameterized by `catalog` / `schema`.

# COMMAND ----------
import json
dbutils.widgets.text("catalog", "main", "Target catalog")
dbutils.widgets.text("schema", "gpo_spend_intelligence", "Target schema")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
print("governing", FQ)

# COMMAND ----------
# ## Column mask: redact member_name for non-admins
spark.sql(f"""CREATE OR REPLACE FUNCTION {FQ}.mask_member(v STRING)
RETURN CASE WHEN is_account_group_member('admins') THEN v ELSE '***REDACTED***' END""")
spark.sql(f"ALTER TABLE {FQ}.spend_lines ALTER COLUMN member_name SET MASK {FQ}.mask_member")
print("mask created + applied to spend_lines.member_name")

# COMMAND ----------
# ## Row-level security: each reviewer sees only their assigned regions
# The demo ACL deliberately assigns the workflow owner to two synthetic regions.
# Production deployments replace these rows with the organization's identity mapping.
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.region_acl AS
  SELECT current_user() AS user_name, 'Midwest' AS region
  UNION ALL
  SELECT current_user() AS user_name, 'West' AS region""")
spark.sql(f"""CREATE OR REPLACE TABLE {FQ}.gold_review_queue AS
  SELECT r.line_id, r.member_id, s.member_name, s.region, r.supplier_id,
         r.category, s.amount, r.risk_prob, r.risk_tier
  FROM {FQ}.gold_maverick_risk r
  INNER JOIN {FQ}.spend_lines s ON r.line_id = s.line_id
  WHERE r.risk_tier = 'High'""")
spark.sql(f"ALTER TABLE {FQ}.gold_review_queue ALTER COLUMN member_name SET MASK {FQ}.mask_member")
spark.sql(f"""CREATE OR REPLACE FUNCTION {FQ}.rf_region(reg STRING)
RETURN is_account_group_member('admins')
    OR reg IN (SELECT region FROM {FQ}.region_acl WHERE user_name = current_user())""")
spark.sql(f"ALTER TABLE {FQ}.gold_review_queue SET ROW FILTER {FQ}.rf_region ON (region)")
print("regional row filter created + applied to gold_review_queue.region")

# COMMAND ----------
# ## Grant schema read to account users
spark.sql(f"GRANT SELECT ON SCHEMA {FQ} TO `account users`")
print("granted SELECT on schema to account users")

# COMMAND ----------
# ## Gold consumption views (what the app, KPIs, and Genie read) - reproducible from spend_lines
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_kpis AS SELECT round(sum(amount)/1e6,1) AS spend_under_mgmt_m, count(*) AS line_count,
  round(100.0*sum(CASE WHEN on_contract THEN amount ELSE 0 END)/sum(amount),1) AS contract_coverage_pct,
  round(100.0*sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/sum(amount),1) AS maverick_pct,
  round(sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/1e6,1) AS maverick_spend_m,
  round(sum(savings)/1e6,1) AS savings_realized_m,
  round(greatest(0.0, 0.85 - sum(CASE WHEN on_contract THEN amount ELSE 0 END)/sum(amount)) * sum(amount) * (sum(savings)/nullif(sum(CASE WHEN on_contract THEN amount ELSE 0 END),0)) / 1e6, 1) AS savings_opportunity_m
  FROM {FQ}.spend_lines""")
# GPO economics: coverage translated into the GPO's OWN contract-admin-fee (CAF) revenue, so the exec
# value story traces to a query. CAF assumption 2.5% (under the 3% federal safe-harbor cap); target 85%.
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_gpo_economics AS
  WITH base AS (SELECT sum(amount) total_spend, sum(CASE WHEN on_contract THEN amount ELSE 0 END) on_contract_spend,
                       sum(savings) member_savings_realized FROM {FQ}.spend_lines),
       p AS (SELECT 0.025 caf_rate, 0.85 coverage_target)
  SELECT round(total_spend/1e6,1) AS spend_under_mgmt_m,
    round(100.0*on_contract_spend/total_spend,1) AS coverage_pct,
    CAST(coverage_target*100 AS INT) AS coverage_target_pct,
    round(greatest(0, coverage_target*total_spend - on_contract_spend)/1e6,1) AS incremental_on_contract_to_target_m,
    caf_rate*100 AS contract_admin_fee_pct,
    round(caf_rate*on_contract_spend/1e6,2) AS gpo_fee_revenue_today_m,
    round(caf_rate*(on_contract_spend+greatest(0,coverage_target*total_spend-on_contract_spend))/1e6,2) AS gpo_fee_revenue_at_target_m,
    round(caf_rate*greatest(0,coverage_target*total_spend-on_contract_spend)/1e6,2) AS incremental_gpo_fee_m,
    round(member_savings_realized/1e6,1) AS member_savings_realized_m
  FROM base, p""")
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_coverage_by_category AS SELECT category, round(sum(amount)/1e6,1) AS spend_m,
  round(100.0*sum(CASE WHEN on_contract THEN amount ELSE 0 END)/sum(amount),1) AS coverage_pct, round(sum(savings)/1e6,2) AS savings_m
  FROM {FQ}.spend_lines GROUP BY category ORDER BY coverage_pct DESC""")
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_member_maverick AS SELECT member_id, member_name, region, round(sum(amount)/1e6,2) AS total_spend_m,
  round(sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/1e6,2) AS off_contract_m, round(100.0*sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/sum(amount),1) AS maverick_pct
  FROM {FQ}.spend_lines GROUP BY member_id, member_name, region ORDER BY maverick_pct DESC""")
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_supplier_scorecard AS SELECT s.supplier_id, s.supplier_name, s.category_specialty, s.compliance_rate, s.quality_score, s.contract_status, s.avg_lead_time_days,
  round(coalesce(sum(l.amount),0)/1e6,2) AS spend_m, CASE WHEN s.compliance_rate < 80 THEN 'Review' WHEN s.compliance_rate < 85 THEN 'Watch' ELSE 'OK' END AS alert_tier
  FROM {FQ}.dim_suppliers s LEFT JOIN {FQ}.spend_lines l USING (supplier_id)
  GROUP BY s.supplier_id, s.supplier_name, s.category_specialty, s.compliance_rate, s.quality_score, s.contract_status, s.avg_lead_time_days""")
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_region_summary AS SELECT region, round(sum(amount)/1e6,1) AS spend_m,
  round(100.0*sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/sum(amount),1) AS maverick_pct, round(sum(savings)/1e6,2) AS savings_m
  FROM {FQ}.spend_lines GROUP BY region ORDER BY maverick_pct DESC""")
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_contract_expirations AS SELECT expiration_status, risk_level, count(*) AS contracts, round(sum(total_annual_value)/1e6,2) AS annual_value_m
  FROM {FQ}.dim_contracts GROUP BY expiration_status, risk_level""")
# temporal analytics view (net-new): monthly spend / off-contract / maverick trend
spark.sql(f"""CREATE OR REPLACE VIEW {FQ}.gold_spend_trend AS SELECT date_trunc('month', order_date) AS month,
  round(sum(amount)/1e6,1) AS spend_m, round(sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/1e6,1) AS off_contract_m,
  round(100.0*sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END)/sum(amount),1) AS maverick_pct
  FROM {FQ}.spend_lines GROUP BY 1 ORDER BY 1""")
print("gold consumption views created (kpis, coverage, member, supplier, region, expirations, spend_trend)")

# COMMAND ----------
# ## Verify the mask is active
masked = spark.sql(f"SELECT member_id, member_name FROM {FQ}.spend_lines LIMIT 5").collect()
for r in masked:
    print(r["member_id"], "->", r["member_name"])
mask_active = any(r["member_name"] == "***REDACTED***" for r in masked)
listed = spark.sql(f"""SELECT column_name, mask_name FROM {CATALOG}.information_schema.column_masks
                       WHERE table_schema = '{SCHEMA}' AND table_name = 'spend_lines'""").collect()
print("registered masks:", [(r["column_name"], r["mask_name"]) for r in listed])
row_filters = spark.sql(f"""SELECT table_name, filter_name FROM {CATALOG}.information_schema.row_filters
                            WHERE table_schema = '{SCHEMA}' AND table_name = 'gold_review_queue'""").collect()
visible_regions = [r["region"] for r in spark.sql(
    f"SELECT DISTINCT region FROM {FQ}.gold_review_queue ORDER BY region").collect()]
print("registered row filters:", [(r["table_name"], r["filter_name"]) for r in row_filters])
print("visible review regions:", visible_regions)

# COMMAND ----------
# ## Genie benchmarks (NL question -> SQL -> answer)
benchmarks = [
    ("What is total spend under management?",
     f"SELECT round(sum(amount),0) AS total_spend FROM {FQ}.spend_lines"),
    ("What is the contract coverage rate?",
     f"SELECT round(100.0*sum(CASE WHEN on_contract THEN amount ELSE 0 END)/sum(amount),1) AS coverage_pct FROM {FQ}.spend_lines"),
    ("Which 5 members have the most off-contract (maverick) spend?",
     f"SELECT member_id, round(sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END),0) AS maverick_spend FROM {FQ}.spend_lines GROUP BY member_id ORDER BY maverick_spend DESC LIMIT 5"),
    ("How many spend lines are flagged High risk by the model?",
     f"SELECT risk_tier, count(*) AS lines FROM {FQ}.gold_maverick_risk GROUP BY risk_tier ORDER BY lines DESC"),
    ("What is off-contract spend by category (top 5)?",
     f"SELECT category, round(sum(CASE WHEN NOT on_contract THEN amount ELSE 0 END),0) AS off_contract FROM {FQ}.spend_lines GROUP BY category ORDER BY off_contract DESC LIMIT 5"),
]
bench_out = []
for q, s in benchmarks:
    rows = spark.sql(s).collect()
    ans = [r.asDict() for r in rows]
    bench_out.append({"q": q, "answer": ans})
    print(f"Q: {q}\n   {ans}")

# COMMAND ----------
out = {"catalog": CATALOG, "schema": SCHEMA, "mask_active": mask_active,
       "masks_registered": len(listed), "row_filters_registered": len(row_filters),
       "visible_review_regions": visible_regions, "benchmarks_run": len(bench_out)}
print(json.dumps(out))
dbutils.notebook.exit(json.dumps({**out, "benchmarks": bench_out}))
