# Databricks notebook source
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# COMMAND ----------

dbutils.widgets.text("dim_product_pipeline_id", "")
dbutils.widgets.text("dim_warehouse_pipeline_id", "")
dbutils.widgets.text("fact_transactions_pipeline_id", "")
dbutils.widgets.text("inventory_historical_pipeline_id", "")

dim_product_pipeline_id = dbutils.widgets.get("dim_product_pipeline_id")
dim_warehouse_pipeline_id = dbutils.widgets.get("dim_warehouse_pipeline_id")
inventory_transaction_pipeline_id = dbutils.widgets.get("fact_transactions_pipeline_id")
inventory_historical_pipeline_id = dbutils.widgets.get("inventory_historical_pipeline_id")

# COMMAND ----------

# Dim Product
dim_product_update = w.pipelines.start_update(pipeline_id=dim_product_pipeline_id, full_refresh=True)
print(f"Dim Product update ID: {dim_product_update.update_id}")

# COMMAND ----------

# Dim Warehouse
dim_warehouse_update = w.pipelines.start_update(pipeline_id=dim_warehouse_pipeline_id, full_refresh=True)
print(f"Dim Warehouse update ID: {dim_warehouse_update.update_id}")

# COMMAND ----------

# Inventory Transactions
inventory_transaction_update = w.pipelines.start_update(pipeline_id=inventory_transaction_pipeline_id, full_refresh=True)
print(f"Inventory Transactions update ID: {inventory_transaction_update.update_id}")

# COMMAND ----------

# Inventory Historical
inventory_historical_update = w.pipelines.start_update(pipeline_id=inventory_historical_pipeline_id, full_refresh=True)
print(f"Inventory Historical update ID: {inventory_historical_update.update_id}")

# COMMAND ----------


