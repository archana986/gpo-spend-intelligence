# Databricks notebook source
# Standard Databricks pattern, all in-workspace: train -> MLflow log -> UC register
# -> alias -> batch-score a gold table. ML runtime (mlflow + sklearn preinstalled),
# autolog OFF (its SHAP hooks hang on wide one-hot data). No input_example.
import json
import pandas as pd
import mlflow, mlflow.sklearn
from mlflow import MlflowClient
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, accuracy_score, precision_score, recall_score

mlflow.autolog(disable=True)
mlflow.set_registry_uri("databricks-uc")
dbutils.widgets.text("catalog", "main", "Target catalog")
dbutils.widgets.text("schema", "gpo_spend_intelligence", "Target schema")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
MODEL = f"{CATALOG}.{SCHEMA}.maverick_risk_model"
cat = ["member_id", "region", "supplier_id", "category"]
# temporal features (net-new): proximity to the category's contract renewal + order month
num = ["quantity", "unit_price", "amount", "days_to_expiry", "order_month"]

# COMMAND ----------
pdf = (spark.table(f"{CATALOG}.{SCHEMA}.spend_lines")
       .select("line_id", *cat, *num, "on_contract").toPandas())
pdf["maverick"] = (~pdf["on_contract"]).astype(int)
X, y = pdf[cat + num], pdf["maverick"]
Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
pre = ColumnTransformer([("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), cat),
                         ("num", StandardScaler(), num)])
clf = Pipeline([("pre", pre),
                ("gbm", HistGradientBoostingClassifier(random_state=42, max_iter=300,
                                                       class_weight="balanced"))])
clf.fit(Xtr, ytr)
proba = clf.predict_proba(Xte)[:, 1]; pred = (proba >= 0.5).astype(int)
metrics = {"auc": round(float(roc_auc_score(yte, proba)), 4),
           "accuracy": round(float(accuracy_score(yte, pred)), 4),
           "precision": round(float(precision_score(yte, pred)), 4),
           "recall": round(float(recall_score(yte, pred)), 4)}

# COMMAND ----------
with mlflow.start_run(run_name="maverick-risk") as run:
    for k, v in metrics.items():
        mlflow.log_metric(k, v)
    sig = mlflow.models.infer_signature(Xtr, clf.predict(Xtr))
    # Let MLflow infer pip requirements from the *installed* env so the serving
    # container installs the exact versions that pickled the model (unpinned reqs
    # caused a version-mismatch "failed to load model" on the endpoint). Inference
    # reads the local env, not the network.
    mlflow.sklearn.log_model(clf, "model", signature=sig,
                             registered_model_name=MODEL)

c = MlflowClient(registry_uri="databricks-uc")
version = max(int(mv.version) for mv in c.search_model_versions(f"name='{MODEL}'"))
c.set_registered_model_alias(MODEL, "champion", version)

# COMMAND ----------
# Batch-score every spend line into a gold table the app/Genie can read
full = pdf.copy()
full["risk_prob"] = clf.predict_proba(full[cat + num])[:, 1].round(4)
full["risk_tier"] = pd.cut(full["risk_prob"], [-0.01, 0.33, 0.66, 1.01],
                           labels=["Low", "Medium", "High"]).astype(str)
gold = full[["line_id", "member_id", "supplier_id", "category", "risk_prob", "risk_tier"]]
(spark.createDataFrame(gold).write.mode("overwrite")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.gold_maverick_risk"))

out = {**metrics, "version": version, "model": MODEL,
       "scored_rows": len(gold),
       "high_risk": int((full["risk_tier"] == "High").sum())}
print(json.dumps(out))
dbutils.notebook.exit(json.dumps(out))
