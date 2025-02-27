# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Project Name: UC Metastore Metadata DR
# MAGIC
# MAGIC ##### Purpose: Find Object Versions in the backup table
# MAGIC
# MAGIC ##### Note:
# MAGIC - This notebook will list all Object Versions in the backup table for a given object name and type
# MAGIC
# MAGIC - Requires:
# MAGIC   - Single user cluster with DBR ML 14+ because of Predictive IO accelerated updates
# MAGIC   - Spark configuration to access the Storage Account in the cluster configs
# MAGIC 
# MAGIC ##### Usage:
# MAGIC - This notebook is meant to be run when you need to find the versions of an object in the backup table
# MAGIC - Input parameters:
# MAGIC   - `src_storage_path`(required): Source storage path for backup
# MAGIC   - `object_name`(required): Name of the object to be searched
# MAGIC   - `object_type`(required): Type of the object to be searched
# MAGIC   - `search_mode`(required): Search mode for the object. Options are `HISTORY` or `LATEST`
# MAGIC   - `search_depth`(required): Search depth in days
# MAGIC   
# MAGIC ##### Revision History:
# MAGIC
# MAGIC | Date | Author | Description |
# MAGIC |---------|----------|---------------|
# MAGIC |04/26/2024| Wagner Silveira - Databricks | Initial Development |

# COMMAND ----------
from pyspark.sql import functions as F
from datetime import timedelta

# COMMAND ----------
OBJECT_TYPES = [
    "CATALOG",
    "SCHEMA",
    "VOLUME",
    "TABLE",
    "VIEW",
    "FUNCTION",
    "STORAGE CREDENTIAL",
    "EXTERNAL LOCATION",
    "RECIPIENT",
    "SHARE",
    "DELTA SHARING SHARE OBJECT"
]

dbutils.widgets.text("src_storage_path", "", "Source Storage Path")
dbutils.widgets.text("object_name", "", "Object Name")
dbutils.widgets.dropdown("object_type", OBJECT_TYPES[0], OBJECT_TYPES, "Object Type")
dbutils.widgets.dropdown("search_mode", "LATEST", ["HISTORY", "LATEST"], "Search Mode")
dbutils.widgets.text("search_depth", "7", "Search Depth in Days")

src_storage_path = dbutils.widgets.get("src_storage_path").strip()
object_name = dbutils.widgets.get("object_name").strip().lower()
object_type = dbutils.widgets.get("object_type").strip()
search_mode = dbutils.widgets.get("search_mode").strip()
search_depth = int(dbutils.widgets.get("search_depth").strip())

if src_storage_path.endswith("/"):
    src_storage_path = src_storage_path[:-1]

# COMMAND ----------
try:
    backup_history = (
        spark.sql(f"DESCRIBE HISTORY delta.`{src_storage_path}/backup_data`")
        .select("version", "timestamp")
        .orderBy("version", ascending=False)
        .collect()
    )

    print(f"Found {len(backup_history)} versions of the backup table")
except Exception as e:
    raise(f"Error while retrieving backup history: {str(e)}")

# COMMAND ----------

final_df = None
latest_version = backup_history[0]["version"]
final_timestamp = backup_history[0]["timestamp"] - timedelta(days=search_depth)

if search_mode == "HISTORY": 
    print(f"Search will happen until {final_timestamp}")

# COMMAND ----------

for row in backup_history:
    version = row["version"]
    timestamp = row["timestamp"]

    if timestamp < final_timestamp and search_mode == "HISTORY":
        print(f"Reached the search depth of {search_depth} days. Stopping search...")
        break

    print(f"Processing version {version}, {timestamp} of {latest_version}")
    
    try:
        backup_table = (
            spark.read.format("delta").option("versionAsOf", version).load(f"{src_storage_path}/backup_data")
            .withColumn("object_name", F.lower(F.col("object_name")))
        )
    except Exception as e:
        print(f"No past versions to read. Stopping search...")
        break

    filtered_backup_table = backup_table.filter(f"object_name = '{object_name}' AND object_type = '{object_type}'")

    if not filtered_backup_table.isEmpty():
        filtered_backup_table = (
            filtered_backup_table.withColumn("version", F.lit(version))
            .withColumn("timestamp", F.lit(timestamp))
        )

        if final_df:
            final_df = final_df.union(filtered_backup_table)
        else:
            final_df = filtered_backup_table

        if search_mode == "LATEST":
            print("Search Mode is LATEST. Object found and stopping search...")
            break

# COMMAND ----------
if final_df:
    final_df = final_df.select("version", "timestamp", "*").orderBy("version", ascending=False)
    final_df.display()
else:
    print(f"No versions found for object {object_name} of type {object_type}")

# COMMAND ----------
dbutils.notebook.exit("Finished Execution.")