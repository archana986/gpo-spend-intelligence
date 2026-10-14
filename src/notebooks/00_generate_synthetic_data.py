# Databricks notebook source
#
# # 00 · Generate synthetic data (deterministic)
# Seeded generator for the GPO Spend Intelligence build. Reproduces the full
# data footprint from nothing: dimensions, reference categories, a 60k-row
# spend fact, and a 500-event PO landing set (JSON) for the Lakeflow pipeline.
#
# **All data is synthetic**  -  fictional GPO *Northwind Health Alliance*.
# No real organization, member, supplier, or PHI.
#
# Parameterized by `catalog` / `schema` widgets  -  no hardcoding, so the same
# notebook regenerates the build into any target (e.g. a fresh repro schema).

# COMMAND ----------
import json
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                               DoubleType, BooleanType, DateType, DecimalType, LongType)

dbutils.widgets.text("catalog", "main", "Target catalog")
dbutils.widgets.text("schema", "gpo_spend_intelligence", "Target schema")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"
SEED = 42
rng = np.random.default_rng(SEED)
print(f"Target: {FQ}  (seed={SEED})")

# COMMAND ----------
# ## Create schema + landing volume (idempotent)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQ}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {FQ}.po_landing")
VOL = f"/Volumes/{CATALOG}/{SCHEMA}/po_landing"
# clean any prior landing files so re-runs are deterministic
try:
    for fi in dbutils.fs.ls(VOL):
        dbutils.fs.rm(fi.path)
except Exception as e:
    print("volume empty / new:", e)
print("landing volume:", VOL)

# COMMAND ----------
# ## Reference categories (fixed  -  the taxonomy the GPO negotiates against)
CATS = [
    ("Med-Surg Supplies",       0.30, 0.035, 0.84, 0.08),
    ("Pharmaceuticals",         0.24, 0.055, 0.90, 0.06),
    ("Lab & Diagnostics",       0.12, 0.045, 0.80, 0.07),
    ("Cardiology / Devices",    0.11, 0.070, 0.86, 0.12),
    ("Orthopedics / Implants",  0.09, 0.080, 0.88, 0.25),
    ("Imaging",                 0.07, 0.040, 0.78, 0.10),
    ("Facilities / MRO",        0.04, 0.025, 0.60, 0.05),
    ("IT / Capital",            0.03, 0.030, 0.55, 0.04),
]
ref_pdf = pd.DataFrame(CATS, columns=["category", "pct_of_spend", "avg_savings",
                                      "base_coverage", "backorder_rate"])
cat_names = ref_pdf["category"].tolist()
cat_weights = ref_pdf["pct_of_spend"].to_numpy()
cat_weights = cat_weights / cat_weights.sum()
base_cov = dict(zip(ref_pdf["category"], ref_pdf["base_coverage"]))
avg_sav = dict(zip(ref_pdf["category"], ref_pdf["avg_savings"]))
(spark.createDataFrame(ref_pdf)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable(f"{FQ}.ref_categories"))
print("ref_categories:", spark.table(f"{FQ}.ref_categories").count())

# COMMAND ----------
# ## dim_members (40 member health systems)
REGIONS = ["Midwest", "Northeast", "Southeast", "Southwest", "West"]
region_w = np.array([0.18, 0.27, 0.10, 0.20, 0.25])
N_MEM = 40
mem_regions = rng.choice(REGIONS, size=N_MEM, p=region_w)
mem = pd.DataFrame({
    "member_id": [f"MBR-{i:02d}" for i in range(1, N_MEM + 1)],
    "member_name": [f"Member Health System {i:02d}" for i in range(1, N_MEM + 1)],
    "region": mem_regions,
    "staffed_beds": rng.integers(80, 1000, N_MEM),
    "maverick_bias": np.round(rng.normal(-0.05, 0.05, N_MEM), 4),
})
mem_schema = StructType([
    StructField("member_id", StringType()), StructField("member_name", StringType()),
    StructField("region", StringType()), StructField("staffed_beds", IntegerType()),
    StructField("maverick_bias", DoubleType())])
mem["staffed_beds"] = mem["staffed_beds"].astype(int)
mem["maverick_bias"] = mem["maverick_bias"].astype(float)
(spark.createDataFrame(mem, schema=mem_schema)
 .withColumn("maverick_bias", F.col("maverick_bias").cast("decimal(17,4)"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dim_members"))
mem_bias = dict(zip(mem["member_id"], mem["maverick_bias"]))
mem_region = dict(zip(mem["member_id"], mem["region"]))
print("dim_members:", spark.table(f"{FQ}.dim_members").count())

# COMMAND ----------
# ## dim_suppliers (120 suppliers)
ADJ = ["Pioneer", "Corven", "Beacon", "Meridian", "Sterling", "Vanguard", "Summit",
       "Apex", "Keystone", "Cardinal", "Atlas", "Crest", "Halcyon", "Nova", "Orion"]
NOUN = ["Diagnostics", "Medical", "Surgical", "Devices", "Labs", "Health", "BioTech",
        "Instruments", "Supply", "Systems", "Imaging", "Therapeutics"]
STATES = ["CA", "TX", "IL", "NY", "PA", "OH", "MA", "MN", "GA", "NC", "WA", "FL"]
STATUS = ["Active", "Expired", "Under Review"]
status_w = np.array([0.68, 0.20, 0.12])
N_SUP = 120
sup = pd.DataFrame({
    "supplier_id": [f"SUP-{i:03d}" for i in range(1, N_SUP + 1)],
    "supplier_name": [f"{ADJ[rng.integers(len(ADJ))]} {NOUN[rng.integers(len(NOUN))]}"
                      for _ in range(N_SUP)],
    "category_specialty": rng.choice(cat_names, size=N_SUP),
    "compliance_rate": np.round(rng.uniform(82, 99, N_SUP), 1),
    "quality_score": np.round(rng.uniform(70, 99, N_SUP), 1),
    "contract_status": rng.choice(STATUS, size=N_SUP, p=status_w),
    "avg_lead_time_days": rng.integers(2, 45, N_SUP),
    "headquarters_state": rng.choice(STATES, size=N_SUP),
})
sup_schema = StructType([
    StructField("supplier_id", StringType()), StructField("supplier_name", StringType()),
    StructField("category_specialty", StringType()), StructField("compliance_rate", DoubleType()),
    StructField("quality_score", DoubleType()), StructField("contract_status", StringType()),
    StructField("avg_lead_time_days", IntegerType()), StructField("headquarters_state", StringType())])
sup["avg_lead_time_days"] = sup["avg_lead_time_days"].astype(int)
sup["compliance_rate"] = sup["compliance_rate"].astype(float)
sup["quality_score"] = sup["quality_score"].astype(float)
(spark.createDataFrame(sup, schema=sup_schema)
 .withColumn("compliance_rate", F.col("compliance_rate").cast("decimal(14,1)"))
 .withColumn("quality_score", F.col("quality_score").cast("decimal(14,1)"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dim_suppliers"))
# suppliers grouped by category, for realistic supplier<->category assignment
sup_by_cat = {c: sup.loc[sup["category_specialty"] == c, "supplier_id"].tolist() for c in cat_names}
all_sups = sup["supplier_id"].tolist()
print("dim_suppliers:", spark.table(f"{FQ}.dim_suppliers").count())

# COMMAND ----------
# ## dim_contracts (200 negotiated contracts)
N_CON = 200
con_sup = rng.choice(all_sups, size=N_CON)
sup_name_map = dict(zip(sup["supplier_id"], sup["supplier_name"]))
sup_cat_map = dict(zip(sup["supplier_id"], sup["category_specialty"]))
eff = pd.to_datetime("2023-01-01") + pd.to_timedelta(rng.integers(0, 900, N_CON), unit="D")
term = rng.choice([1, 2, 3], size=N_CON, p=[0.3, 0.4, 0.3])
exp = eff + pd.to_timedelta(term * 365, unit="D")
today = pd.Timestamp("2026-01-01")
dte = np.asarray((exp - today).days)
con = pd.DataFrame({
    "contract_id": [f"CON-{i:04d}" for i in range(1, N_CON + 1)],
    "supplier_id": con_sup,
    "supplier_name": [sup_name_map[s] for s in con_sup],
    "category_specialty": [sup_cat_map[s] for s in con_sup],
    "contract_type": rng.choice(["Sole Source", "Dual Source", "Multi Source"], size=N_CON, p=[0.25, 0.35, 0.4]),
    "effective_date": eff.date,
    "expiration_date": exp.date,
    "term_years": term.astype(int),
    "auto_renew": rng.choice([True, False], size=N_CON, p=[0.6, 0.4]),
    "facility_count": rng.integers(1, 40, N_CON).astype(int),
    "total_annual_value": rng.integers(50_000, 5_000_000, N_CON).astype(int),
    "days_to_expiration": dte.astype(int),
    "expiration_status": np.where(dte < 0, "Expired", np.where(dte < 90, "Expiring Soon", "Active")),
    "risk_level": np.where(dte < 0, "High", np.where(dte < 90, "Medium", "Low")),
    "fee_assessment": rng.choice(["Standard", "Reduced", "Waived"], size=N_CON, p=[0.7, 0.2, 0.1]),
})
con_schema = StructType([
    StructField("contract_id", StringType()), StructField("supplier_id", StringType()),
    StructField("supplier_name", StringType()), StructField("category_specialty", StringType()),
    StructField("contract_type", StringType()), StructField("effective_date", DateType()),
    StructField("expiration_date", DateType()), StructField("term_years", IntegerType()),
    StructField("auto_renew", BooleanType()), StructField("facility_count", IntegerType()),
    StructField("total_annual_value", IntegerType()), StructField("days_to_expiration", IntegerType()),
    StructField("expiration_status", StringType()), StructField("risk_level", StringType()),
    StructField("fee_assessment", StringType())])
(spark.createDataFrame(con, schema=con_schema)
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.dim_contracts"))
print("dim_contracts:", spark.table(f"{FQ}.dim_contracts").count())

# COMMAND ----------
# ## spend_lines (60k rows  -  the analytical fact)
# `on_contract` is driven by category coverage minus member maverick bias plus
# noise, so the ML model finds real-but-imperfect signal (target AUC ~0.65-0.72,
# overall off-contract ~21%).
N_LINES = 60_000
mem_ids = mem["member_id"].to_numpy()
line_cats = rng.choice(cat_names, size=N_LINES, p=cat_weights)
line_mem = rng.choice(mem_ids, size=N_LINES)
# --- temporal signal (net-new): order date, contract-expiry pressure, seasonality ---
# Drawn from a SEPARATE RNG stream so quantities/prices/amounts are unchanged; only the
# on-contract OUTCOME shifts with time, giving the model a genuine temporal feature to rank on.
trng = np.random.default_rng(SEED + 7)
order_offset = trng.integers(0, 365, N_LINES)
order_date = pd.to_datetime("2025-01-01") + pd.to_timedelta(order_offset, unit="D")
order_month = order_date.month.to_numpy().astype(int)
# each category's GPO contract renews on a staggered day of the year
cat_expiry_doy = {c: int(40 + 300 * k / max(1, len(cat_names) - 1)) for k, c in enumerate(cat_names)}
days_to_expiry = (order_offset - np.array([cat_expiry_doy[c] for c in line_cats])).astype(int)
# maverick pressure ramps as an order nears/passes its category's contract expiry (logistic)
expiry_pressure = 0.20 / (1.0 + np.exp(-days_to_expiry / 25.0))
seasonal = np.where(order_month >= 10, 0.06, 0.0)          # mild Q4 budget-flush drift
# center both so ANNUAL coverage is essentially unchanged (temporal redistribution, not a level shift)
expiry_pressure = expiry_pressure - expiry_pressure.mean()
seasonal = seasonal - seasonal.mean()
line_sup, on_contract = [], np.empty(N_LINES, dtype=bool)
for i in range(N_LINES):
    c = line_cats[i]
    pool = sup_by_cat.get(c) or all_sups
    line_sup.append(pool[rng.integers(len(pool))])
    p_on = (base_cov[c] - float(mem_bias[line_mem[i]]) - 0.10
            - float(expiry_pressure[i]) - float(seasonal[i]) + rng.normal(0, 0.10))
    on_contract[i] = rng.random() < np.clip(p_on, 0.05, 0.98)
line_sup = np.array(line_sup)
quantity = rng.integers(1, 60, N_LINES)
list_price = np.round(rng.gamma(2.0, 350, N_LINES) + 20, 2)
# on-contract lines get the negotiated savings; maverick lines pay closer to list
sav = np.array([avg_sav[c] for c in line_cats])
disc = np.where(on_contract, sav + rng.uniform(-0.01, 0.02, N_LINES),
                rng.uniform(-0.03, 0.01, N_LINES))
unit_price = np.round(list_price * (1 - disc), 2)
amount = np.round(unit_price * quantity, 2)
baseline_list_amount = np.round(list_price * quantity, 2)
savings = np.round(baseline_list_amount - amount, 2)
mem_name_map = dict(zip(mem["member_id"], mem["member_name"]))
con_type = rng.choice(["Sole Source", "Dual Source", "Multi Source"], size=N_LINES)
delivery = rng.choice(["Delivered", "In Transit", "Backordered"], size=N_LINES, p=[0.88, 0.09, 0.03])
spend = pd.DataFrame({
    "line_id": [f"LN-{i:07d}" for i in range(N_LINES)],
    "order_date": order_date.date,
    "member_id": line_mem,
    "member_name": [mem_name_map[m] for m in line_mem],
    "region": [mem_region[m] for m in line_mem],
    "supplier_id": line_sup,
    "category": line_cats,
    "contract_type": con_type,
    "on_contract": on_contract,
    "quantity": quantity.astype(int),
    "list_price": list_price,
    "unit_price": unit_price,
    "amount": amount,
    "baseline_list_amount": baseline_list_amount,
    "savings": savings,
    "delivery_status": delivery,
    "days_to_expiry": days_to_expiry,
    "order_month": order_month,
})
spend_schema = StructType([
    StructField("line_id", StringType()), StructField("order_date", DateType()),
    StructField("member_id", StringType()), StructField("member_name", StringType()),
    StructField("region", StringType()), StructField("supplier_id", StringType()),
    StructField("category", StringType()), StructField("contract_type", StringType()),
    StructField("on_contract", BooleanType()), StructField("quantity", IntegerType()),
    StructField("list_price", DoubleType()), StructField("unit_price", DoubleType()),
    StructField("amount", DoubleType()), StructField("baseline_list_amount", DoubleType()),
    StructField("savings", DoubleType()), StructField("delivery_status", StringType()),
    StructField("days_to_expiry", IntegerType()), StructField("order_month", IntegerType())])
for c in ("list_price", "unit_price", "amount", "baseline_list_amount", "savings"):
    spend[c] = spend[c].astype(float)
(spark.createDataFrame(spend, schema=spend_schema)
 .withColumn("list_price", F.col("list_price").cast("decimal(15,2)"))
 .withColumn("baseline_list_amount", F.col("baseline_list_amount").cast("decimal(27,2)"))
 .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{FQ}.spend_lines"))
oc = spend["on_contract"].mean()
print(f"spend_lines: {len(spend)}  off_contract={1-oc:.4f}  total_spend=${spend['amount'].sum():,.0f}")

# COMMAND ----------
# ## Land 500 PO/point-of-use events as JSON (source for the Lakeflow pipeline)
N_EVT = 500
ev_cat = rng.choice(cat_names, size=N_EVT, p=cat_weights)
ev_mem = rng.choice(mem_ids, size=N_EVT)
ev_sup, ev_on = [], np.empty(N_EVT, dtype=bool)
for i in range(N_EVT):
    pool = sup_by_cat.get(ev_cat[i]) or all_sups
    ev_sup.append(pool[rng.integers(len(pool))])
    p_on = base_cov[ev_cat[i]] - float(mem_bias[ev_mem[i]]) - 0.10 + rng.normal(0, 0.10)
    ev_on[i] = rng.random() < np.clip(p_on, 0.05, 0.98)
ev_qty = rng.integers(1, 40, N_EVT)
ev_unit = np.round(rng.gamma(2.0, 300, N_EVT) + 15, 2)
events = []
base_ts = pd.Timestamp("2026-01-05T08:00:00")
for i in range(N_EVT):
    events.append({
        "event_id": f"EVT-{i:06d}",
        "event_type": str(rng.choice(["PO_CREATED", "POINT_OF_USE"], p=[0.6, 0.4])),
        "event_time": (base_ts + pd.to_timedelta(int(rng.integers(0, 72)), unit="h")).isoformat(),
        "member_id": str(ev_mem[i]),
        "supplier_id": str(ev_sup[i]),
        "category": str(ev_cat[i]),
        "item_sku": f"SKU-{rng.integers(10000, 99999)}",
        "quantity": int(ev_qty[i]),
        "unit_cost": float(ev_unit[i]),
        "amount": float(round(ev_unit[i] * ev_qty[i], 2)),
        "contract_id": f"CON-{rng.integers(1, N_CON + 1):04d}",
        "on_contract": bool(ev_on[i]),
    })
# write in 5 files of 100 to exercise Auto Loader multi-file ingest
for b in range(5):
    chunk = events[b * 100:(b + 1) * 100]
    payload = "\n".join(json.dumps(e) for e in chunk)
    dbutils.fs.put(f"{VOL}/po_events_{b:04d}.json", payload, overwrite=True)
print(f"landed {N_EVT} PO events across 5 JSON files in {VOL}")

# COMMAND ----------
# ## Summary (asserted by the test job)
summary = {
    "catalog": CATALOG, "schema": SCHEMA, "seed": SEED,
    "dim_members": spark.table(f"{FQ}.dim_members").count(),
    "dim_suppliers": spark.table(f"{FQ}.dim_suppliers").count(),
    "dim_contracts": spark.table(f"{FQ}.dim_contracts").count(),
    "ref_categories": spark.table(f"{FQ}.ref_categories").count(),
    "spend_lines": spark.table(f"{FQ}.spend_lines").count(),
    "off_contract_pct": round(float(1 - oc) * 100, 2),
    "po_events_landed": N_EVT,
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
