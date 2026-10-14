# Setup: GPO Spend Intelligence

Use this project in **your** Databricks workspace. All data is synthetic Northwind Health Alliance. No customer KPIs are shipped in this pack.

## Prerequisites

| Requirement | How to check |
|---|---|
| Databricks CLI 0.292 or later | `databricks -v` |
| OAuth login | `databricks auth login --host <your-workspace-url>` (do not use a PAT) |
| `CREATE SCHEMA` on the target catalog | The job creates a schema, not a catalog |
| Serverless jobs and Spark Declarative Pipelines | Workspace entitlement |
| A SQL warehouse with `CAN_USE` | ID from **SQL > Warehouses** |
| Model Serving and a Foundation Model endpoint | Serving UI |
| An existing Genie space over the Gold views | Create from `resources/genie_space.serialized.json`, then pass `genie_space_id` |
| An existing Lakebase project | Needed only if you start the app |

The CLI profile must match **your** workspace URL. This repo does not pin a host.

## Configure

```bash
databricks bundle validate --strict -t dev \
  --var catalog=<catalog> \
  --var warehouse_id=<warehouse_id> \
  --var genie_space_id=<genie_space_id> \
  --var lakebase_project=<lakebase_project>
```

| Variable | Default | What to set |
|---|---|---|
| `catalog` | `main` | Catalog where you can create a schema |
| `schema` | `gpo_spend_intelligence` | Any writable schema name |
| `warehouse_id` | none | Required |
| `genie_space_id` | none | Required for the app and the Genie validation check |
| `lakebase_project` | none | Required for the app |
| `endpoint` | `gpo-maverick-risk` | Serving endpoint name |
| `llm_endpoint` | `databricks-meta-llama-3-3-70b-instruct` | Briefs model |
| `app_name` | `gpo-spend-intelligence` | App resource name |

## Deploy and run

```bash
databricks bundle deploy -t dev \
  --var catalog=<catalog> \
  --var warehouse_id=<warehouse_id> \
  --var genie_space_id=<genie_space_id> \
  --var lakebase_project=<lakebase_project>

databricks bundle run gpo_build -t dev
```

Then optionally `databricks bundle run gpo_app -t dev`.

Point `resources/genie_space.serialized.json` at the same catalog and schema before you create the space.

## Known limitations

| Limitation | What to do |
|---|---|
| Genie and Lakebase are not created by the bundle | Create them first, then pass IDs |
| Source is a Unity Catalog volume, not live Event Hubs | Swap the generator if you bring your own files |
| Model metrics are on synthetic labels | Do not treat AUC or recall as production calibration |
| Gold dollar amounts are generator output | Do not present them as customer savings |
| App service principal needs grants | `USE CATALOG`, `USE SCHEMA`, `SELECT`, Genie `CAN_RUN` |
| Serving deploy dominates runtime | Plan for tens of minutes on the first build |

## Cleanup

```bash
databricks bundle destroy -t dev
```

The schema, registered model, serving endpoint, Genie space, and Lakebase project may need separate delete steps. Do not drop shared resources.

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Validate asks for `warehouse_id` | Required variable | Pass `--var warehouse_id=...` |
| App build fails on `DESCRIBE QUERY` | App SP cannot read the schema | Grant catalog/schema/select to the app client ID |
| Validate task fails on Genie | Empty or wrong `genie_space_id` | Create the space on the Gold views, then rerun |
| Serving task hangs or fails | Endpoint name collision or quota | Override `endpoint` |
