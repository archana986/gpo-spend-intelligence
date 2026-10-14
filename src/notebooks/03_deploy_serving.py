# Databricks notebook source
#
# # 03 · Deploy Model Serving endpoint (from the champion version)
# Creates or updates a Model Serving endpoint pointed at the UC model's
# `champion` alias. Parameterized by `catalog` / `schema` / `endpoint`.
# Waits for READY so the downstream validation can query it.

# COMMAND ----------
import json, time
from mlflow import MlflowClient
from mlflow.deployments import get_deploy_client

dbutils.widgets.text("catalog", "main", "Target catalog")
dbutils.widgets.text("schema", "gpo_spend_intelligence", "Target schema")
dbutils.widgets.text("endpoint", "gpo-maverick-risk", "Serving endpoint name")
CATALOG, SCHEMA = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
ENDPOINT = dbutils.widgets.get("endpoint")
MODEL = f"{CATALOG}.{SCHEMA}.maverick_risk_model"

reg = MlflowClient(registry_uri="databricks-uc")
champ = reg.get_model_version_by_alias(MODEL, "champion")
VER = champ.version
print(f"champion version = {VER} of {MODEL} -> endpoint {ENDPOINT}")

# COMMAND ----------
deploy = get_deploy_client("databricks")
config = {
    "served_entities": [{
        "entity_name": MODEL, "entity_version": VER,
        "workload_size": "Small", "scale_to_zero_enabled": True,
    }],
    "traffic_config": {"routes": [
        {"served_model_name": f"maverick_risk_model-{VER}", "traffic_percentage": 100}
    ]},
}
existing = [e["name"] for e in deploy.list_endpoints()]
if ENDPOINT in existing:
    print("updating existing endpoint config")
    deploy.update_endpoint(endpoint=ENDPOINT, config=config)
else:
    print("creating endpoint")
    deploy.create_endpoint(name=ENDPOINT, config=config)

# COMMAND ----------
# ## Wait for READY (poll both ready + config_update)
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
deadline = time.time() + 20 * 60
state = {}
while time.time() < deadline:
    ep = w.serving_endpoints.get(ENDPOINT)
    ready = ep.state.ready.value if ep.state and ep.state.ready else None
    upd = ep.state.config_update.value if ep.state and ep.state.config_update else None
    state = {"ready": ready, "config_update": upd}
    print(state)
    if ready == "READY" and upd == "NOT_UPDATING":
        break
    time.sleep(20)

# COMMAND ----------
out = {"endpoint": ENDPOINT, "model": MODEL, "version": VER, **state}
print(json.dumps(out))
dbutils.notebook.exit(json.dumps(out))
