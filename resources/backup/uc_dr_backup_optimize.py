# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Project Name: UC Metastore Metadata DR
# MAGIC
# MAGIC ##### Purpose: Optimize UC Metastore Metadata backups table
# MAGIC
# MAGIC ##### Note: 
# MAGIC - Requires:
# MAGIC   - DBR 14+ because of Predictive IO accelerated updates
# MAGIC   - Spark configuration to access the Storage Account in the cluster configs
# MAGIC 
# MAGIC ##### Usage:
# MAGIC - This notebook is meant to be run periodically to optimize UC Metastore Metadata backup table
# MAGIC - Input parameters:
# MAGIC   - `dest_storage_path`(required): Destination storage path for backup
# MAGIC   - `backup_history_in_days`(optional): Backup history in days
# MAGIC   
# MAGIC ##### Revision History:
# MAGIC
# MAGIC | Date | Author | Description |
# MAGIC |---------|----------|---------------|
# MAGIC |02/29/2024| Wagner Silveira - Databricks | Initial Development |

# COMMAND ----------
import re 

# COMMAND ----------
dbutils.widgets.text("dest_storage_path", "abfss://dr-uc-metastore-backup@dbkucmetastoreeusadls2dr.dfs.core.windows.net/", "Base destination path for backup")
dbutils.widgets.text("backup_history_in_days", "30", "Backup history in days")

dest_storage_path = dbutils.widgets.get("dest_storage_path")
backup_history_in_days = dbutils.widgets.get("backup_history_in_days")

dest_storage_path = dest_storage_path.strip()
dest_storage_path = dest_storage_path[:-1] if dest_storage_path.endswith("/") else dest_storage_path
pattern = re.compile(r"^abfss:\/\/[a-zA-Z0-9\-]+@[a-zA-Z0-9\-]+\.dfs\.core\.windows\.net.*$")
if not pattern.match(dest_storage_path):
    raise ValueError("The destination storage path must be in the format abfss://<container>@<storage-account>.dfs.core.windows.net/base_path")

if not backup_history_in_days.isdigit():
    raise ValueError("The backup history in days must be a positive integer number")
elif int(backup_history_in_days) < 7:
    raise ValueError("The backup history in days must be at least 7 days")
# COMMAND ----------
# set time travel properties
spark.conf.set("spark.databricks.delta.deletedFileRetentionDuration", f"interval {backup_history_in_days} days")
spark.conf.set("spark.databricks.delta.logRetentionDuration", f"interval {backup_history_in_days} days")

# COMMAND ----------
# Only VACUUM is executed to remove no longer used data files
# OPTIMIZE command is not needed because it is optimized at write time, besides being overwritten every time 
spark.sql(f"VACUUM delta.`{dest_storage_path}/backup_data`")

# COMMAND ----------
dbutils.notebook.exit("UC DR Backup optimization complete")