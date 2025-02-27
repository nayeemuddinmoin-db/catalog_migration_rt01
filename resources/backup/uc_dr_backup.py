# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Project Name: UC Metastore Metadata DR
# MAGIC
# MAGIC ##### Purpose: Take UC Metastore Metadata backups and store in a table
# MAGIC
# MAGIC ##### Note:
# MAGIC - First execution run should be expensive as it will backup the whole UC Metastore Metadata
# MAGIC - Subsequent runs should be faster as for most of the objects, only the new records will be backed up
# MAGIC - Full metastore and objects access is required
# MAGIC   - User must be metastore adimn
# MAGIC   - User must have access to all objects (ALL PRIVILEGES in catalogs and other objects)
# MAGIC - Requires:
# MAGIC   - jinja2 python package (~3.1.3)
# MAGIC   - databricks-sdk python package (~0.25.1)
# MAGIC   - msal python package (1.28.0)
# MAGIC   - custom sendemail wheel package from PepsiCo
# MAGIC   - Single user cluster with DBR ML 14+ because of Predictive IO accelerated updates
# MAGIC   - Spark configuration to access the Storage Account in the cluster configs
# MAGIC 
# MAGIC ##### Usage:
# MAGIC - This notebook is meant to be run periodically to backup UC Metastore Metadata
# MAGIC - Input parameters:
# MAGIC   - `dest_storage_path`(required): Destination storage path for backup
# MAGIC   - `backup_mode`(optional): FRESH_BACKUP OR INCREMENTAL_BACKUP
# MAGIC   - `backup_history_in_days`(optional): Backup history in days
# MAGIC   - `email_credential_scope`(optional): Email Credential Databricks Secret Scope (expects a key named `email-secret` inside of this scope)
# MAGIC   
# MAGIC ##### Revision History:
# MAGIC
# MAGIC | Date | Author | Description |
# MAGIC |---------|----------|---------------|
# MAGIC |02/29/2024| Wagner Silveira - Databricks | Initial Development |

# COMMAND ----------
import pathlib
import sys
import importlib
import logging
import inspect
import re

import datetime as dt
from typing import List, Callable
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import DataFrame, functions as F
from functools import reduce
from itertools import repeat

start_time = dt.datetime.now()

path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path = "/Workspace" + path if not path.startswith("/Workspace") else path

modules_path = pathlib.Path(path).joinpath("../../../src").resolve()

# Allow for execution outside of Databricks Repos directory
sys.path.append(str(modules_path))

from dr.uc_dr_objects.objects import BackupMode
from dr.utils.uc_dr_checks import uc_dr_pre_checks, uc_dr_post_checks

# COMMAND ----------
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------
dbutils.widgets.text("dest_storage_path", "abfss://dr-uc-metastore-backup@dbkucmetastoreeusadls2dr.dfs.core.windows.net/", "Base destination path for backup")
dbutils.widgets.dropdown("backup_mode", BackupMode.INCREMENTAL_BACKUP.value, [BackupMode.FRESH_BACKUP.value, BackupMode.INCREMENTAL_BACKUP.value])
dbutils.widgets.text("backup_history_in_days", "30", "Backup history in days")
dbutils.widgets.text("email_credential_scope", "edapemail", "Email Credential Databricks Secret Scope")

dest_storage_path = dbutils.widgets.get("dest_storage_path")
backup_mode = dbutils.widgets.get("backup_mode")
backup_history_in_days = dbutils.widgets.get("backup_history_in_days")
email_credential_scope = dbutils.widgets.get("email_credential_scope").strip()

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
# Enable cache since many tables are read more than once
spark.conf.set("spark.databricks.io.cache.enabled", "true")
# Spark will determine the best number of shuffle partitions since there are complex joins
spark.conf.set("spark.sql.shuffle.partitions", "auto")
# Deletion Vectors are a performance optimization
spark.conf.set("spark.databricks.delta.enableDeletionVectors.enabled", "true") # Requires DBR 14.0+
# Guarantee that the data is written in the most optimized way
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")


# COMMAND ----------
# this list define the objects to be backed up
objects_map = [
    ("dr.uc_dr_objects.storage_credentials", "StorageCredentials"),
    ("dr.uc_dr_objects.storage_credentials", "StorageCredentialPrivileges"),

    ("dr.uc_dr_objects.external_locations", "ExternalLocations"),
    ("dr.uc_dr_objects.external_locations", "ExternalLocationPrivileges"),

    ("dr.uc_dr_objects.catalogs", "Catalogs"),
    ("dr.uc_dr_objects.catalogs", "CatalogPrivileges"),
    ("dr.uc_dr_objects.catalogs", "CatalogTags"),

    ("dr.uc_dr_objects.schemas", "Schemas"),
    ("dr.uc_dr_objects.schemas", "SchemaPrivileges"),
    ("dr.uc_dr_objects.schemas", "SchemaTags"),

    ("dr.uc_dr_objects.tables", "Tables"),
    ("dr.uc_dr_objects.tables", "TablePrivileges"),
    ("dr.uc_dr_objects.tables", "TableTags"),

    ("dr.uc_dr_objects.views", "Views"),
    ("dr.uc_dr_objects.views", "ViewPrivileges"),
    ("dr.uc_dr_objects.views", "ViewTags"),

    ("dr.uc_dr_objects.volumes", "Volumes"),
    ("dr.uc_dr_objects.volumes", "VolumePrivileges"),
    ("dr.uc_dr_objects.volumes", "VolumeTags"),

    ("dr.uc_dr_objects.functions", "Functions"),
    ("dr.uc_dr_objects.functions", "FunctionsPrivileges"),
    
    ("dr.uc_dr_objects.delta_sharing_recipients", "DSRecipients"),
    ("dr.uc_dr_objects.delta_sharing_shares", "DSShares"),
    ("dr.uc_dr_objects.delta_sharing_shares", "DSShareRecipientPrivileges"),
    ("dr.uc_dr_objects.delta_sharing_shares", "DSShareObjects"),

    ("dr.uc_dr_objects.registered_models", "UCRegisteredModels"),
]

# COMMAND ----------

def parallel_backup(function: Callable, objects: List[tuple], start_ts: dt.datetime, backup_mode: BackupMode, threads: int = 8) -> DataFrame:
    """
    Parallelize the backup process

    Args:
        function (Callable): The function to parallelize
        objects (List[tuple]): The list of objects to parallelize
        start_ts (dt.datetime): The start timestamp
        backup_mode (BackupMode): The backup mode
        threads (int): The number of threads to use

    Returns:
        DataFrame: The backup DataFrame
    """

    # Will call objects to be backed up in a parallel way
    with ThreadPoolExecutor(max_workers=threads) as executor:
        results = [result for result in executor.map(function, objects, repeat(start_ts), repeat(backup_mode))]
        final_df = reduce(DataFrame.union, results)

    return final_df

def object_backup(obj_details: tuple, start_ts: dt.datetime, backup_mode: BackupMode) -> DataFrame:
    """
    Backup the object

    Args:
        obj_details (tuple): The object to backup
        start_ts (dt.datetime): The start timestamp
        backup_mode (BackupMode): The backup mode

    Returns:
        DataFrame: The backup DataFrame
    """
    module, obj = obj_details
    try:
        obj_instance = getattr(importlib.import_module(module), obj)()
        
        logger.info(f"Reading {obj_instance.object_type.value} from {dest_storage_path}")

        if inspect.signature(obj_instance.read).parameters.get("dest_storage_path", None):
            # Read method is called to read the data to be backed up
            return obj_instance.read(dest_storage_path=f"{dest_storage_path}/backup_data", start_ts=start_ts, backup_mode=backup_mode)
        else:
            return obj_instance.read(start_ts=start_ts)
    except Exception as e:
        logger.warning(f"Error importing module {module}: {str(e)}")
        raise e
# COMMAND ----------
# Run pre-checks to guarantee that the storage accounts are accessible and catalogs are available
uc_dr_pre_checks(f"{dest_storage_path}/backup_data")

# COMMAND ----------
# Executes backup
df = parallel_backup(object_backup, objects_map, start_time, BackupMode(backup_mode))

# COMMAND ----------
# df.display()
(
    df.filter(F.col("restore_ddl").isNotNull() | F.col("api_payload").isNotNull())
      .write.mode("overwrite").format("delta").save(f"{dest_storage_path}/backup_data")
)

# Run post-checks to analyze the backup
uc_dr_post_checks(df, f"{dest_storage_path}/backup_data", email_credential_scope)

finish_time = dt.datetime.now()

# COMMAND ----------
dbutils.notebook.exit(f"Backup successfully completed. Date: {finish_time.date()}, Execution time: {finish_time-start_time}")
