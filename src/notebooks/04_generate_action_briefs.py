# Databricks notebook source
# MAGIC %md
# MAGIC # GenAI action briefs  -  turn each high-risk line into a recommended action
# MAGIC Reads the model's High-risk lines from `gold_maverick_risk` and uses a Databricks
# MAGIC Foundation Model (`ai_query`) to generate a one-sentence, grounded action recommendation
# MAGIC per line, materialized to `gold_action_briefs`. This is the GenAI "act" layer on top of
# MAGIC the ML risk score: the review queue tells the analyst not just what is risky but what to do.

# COMMAND ----------

dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "gpo_spend_intelligence")
dbutils.widgets.text("llm_endpoint", "databricks-meta-llama-3-3-70b-instruct")
dbutils.widgets.text("top_n", "25")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
LLM = dbutils.widgets.get("llm_endpoint")
TOP_N = int(dbutils.widgets.get("top_n"))
FQ = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# One grounded LLM call per high-risk line via ai_query (governed tables in, NL action out).
spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.gold_action_briefs AS
SELECT
  r.line_id, r.member_id, r.category, round(r.risk_prob, 3) AS risk_prob,
  ai_query('{LLM}',
    CONCAT(
      'You are a healthcare GPO contract analyst. A purchase line is flagged HIGH maverick ',
      '(off-contract) risk. Member ', r.member_id, ', category "', r.category, '", model risk ',
      round(r.risk_prob, 2),
      '. In ONE concise sentence, recommend the specific action to move this spend back ',
      'on-contract. No preamble.')) AS action_brief,
  current_timestamp() AS generated_at
FROM {FQ}.gold_maverick_risk r
WHERE r.risk_tier = 'High'
ORDER BY r.risk_prob DESC
LIMIT {TOP_N}
""")

n = spark.table(f"{FQ}.gold_action_briefs").count()
print(f"gold_action_briefs rows: {n}")
display(spark.sql(f"SELECT member_id, category, risk_prob, action_brief FROM {FQ}.gold_action_briefs ORDER BY risk_prob DESC LIMIT 5"))

# COMMAND ----------

# AI grades AI: an automated LLM-as-judge quality gate over the GenAI action layer. A second FM call
# scores each recommendation 1-5 for actionability, materialized to a queryable table - so a weak brief
# is caught by a scored gate, not by chance. This is the AI-native QA step (not human spot-checks).
spark.sql(f"""
CREATE OR REPLACE TABLE {FQ}.gold_action_brief_quality AS
SELECT line_id, member_id, category, risk_prob, action_brief,
  CAST(regexp_extract(ai_query('{LLM}',
    CONCAT('You are a QA reviewer scoring a procurement action recommendation for a HIGH off-contract-risk ',
           'purchase. Category: ', category, '. Recommendation: ', action_brief,
           '. Score 1 to 5 how SPECIFIC and ACTIONABLE it is for a category manager ',
           '(5 = concrete executable next step; 1 = vague). Reply with ONLY the integer.')),
    '([1-5])', 1) AS INT) AS actionability_score,
  current_timestamp() AS judged_at
FROM {FQ}.gold_action_briefs
""")
q = spark.sql(f"SELECT count(*) briefs, round(avg(actionability_score),2) avg_score, sum(CASE WHEN actionability_score>=4 THEN 1 ELSE 0 END) high_quality FROM {FQ}.gold_action_brief_quality").collect()[0]
print(f"quality gate: {q['briefs']} judged, avg {q['avg_score']}, {q['high_quality']} high-quality (>=4)")

# COMMAND ----------

dbutils.notebook.exit(f'{{"table": "{FQ}.gold_action_briefs", "rows": {n}, "llm": "{LLM}", "quality_gate": {{"avg": {q["avg_score"]}, "high_quality": {q["high_quality"]}}}}}')
