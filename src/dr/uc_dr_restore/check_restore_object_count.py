# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ##### Project Name: UC DR Restore
# MAGIC 
# MAGIC ##### Purpose: Runs DR Restore to re-create UC objects from the DR Backup table
# MAGIC 
# MAGIC ##### Prerequisite: 
# MAGIC - Execute batch_job_execution_start first before running this notebook to make an entry in the BatchJob table and initialize BatchJobControl table
# MAGIC - Full metastore and objects access is required
# MAGIC   - User must be metastore adimn
# MAGIC   - User must have access to all objects (ALL PRIVILEGES in catalogs and other objects)
# MAGIC - Requires:
# MAGIC   - databricks-sdk python package (~0.20.0)
# MAGIC   - Single user cluster with DBR ML 14+ because of Predictive IO accelerated updates
# MAGIC   - Enabling Photon is recommended for better performance
# MAGIC   - Spark configuration to access the Storage Account in the cluster configs
# MAGIC 
# MAGIC ##### Usage:
# MAGIC - This notebook is meant to be run on an adhoc basis whenever a restore needs to be performed
# MAGIC - Input parameters:
# MAGIC   - `input_json_str`(required): UC objects to be restored
# MAGIC   - `dr_adls_root_path`(required): Destination storage path for backup
# MAGIC   - `time_travel_option`(optional): Possible values are version and timestamp. This gives an option to run the restore process from previous version of backup table
# MAGIC   - `time_travel_value`(optional): Version number or timestamp value for the timetravel option for backup table
# MAGIC 
# MAGIC 
# MAGIC ##### Sample JSON string for `input_json_str` parameter:
# MAGIC `Note:` * stands for all objects or provide object names in the list []
# MAGIC 
# MAGIC 1. Restore all uc objects from backup
# MAGIC ```
# MAGIC {
# MAGIC 	"metastore": "*"
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC 2. Restore all uc objects under a specific catalog from backup
# MAGIC 
# MAGIC ```
# MAGIC {    
# MAGIC 	"metastore": {
# MAGIC 		"catalog":{	
# MAGIC 			"dbk_test1": "*"
# MAGIC 			}
# MAGIC 
# MAGIC 	}
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC 3. Restore some selected uc objects from backup
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC   "metastore": {
# MAGIC     "share": [
# MAGIC       "share_01"
# MAGIC     ],
# MAGIC     "storage credential": ["st_cred01"],
# MAGIC     "external location": "*",
# MAGIC     "catalog": {
# MAGIC       "catalog_dr01": "*",
# MAGIC       "catalog_drop_082": "*",
# MAGIC       "catalog_dr02": {
# MAGIC         "schema": {
# MAGIC           "catalog_dr02.database_dr02": {
# MAGIC             "table": [
# MAGIC               "catalog_dr02.database_dr02.tpch_customer"
# MAGIC             ]
# MAGIC           }
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC 4. Some more template 
# MAGIC 
# MAGIC ```
# MAGIC {
# MAGIC   "metastore": {
# MAGIC     "storage credential": <>,
# MAGIC     "external location": <>,
# MAGIC     "connection": <>,
# MAGIC     "share": <>,
# MAGIC     "recipient": <>,
# MAGIC     "catalog": {
# MAGIC       "cat_01": {
# MAGIC         "schema": {
# MAGIC           "cat_01.db_01": {
# MAGIC             "table": <>,
# MAGIC             "view": <>,
# MAGIC             "volume": <>,
# MAGIC             "function": <>,
# MAGIC             "registered model": <>
# MAGIC           }
# MAGIC         }
# MAGIC       }
# MAGIC     }
# MAGIC   }
# MAGIC }
# MAGIC ```
# MAGIC 
# MAGIC ##### Revision History:
# MAGIC 
# MAGIC | Date | Author | Description |
# MAGIC |---------|----------|---------------|
# MAGIC |02/29/2024| Nayeemuddin Moinuddin - Databricks | Initial Development |

# COMMAND ----------
from utilities.restore_restartability_logging import *
from restore_manager import *
from utilities.helper_functions import *
from pyspark.sql import SparkSession
from functools import reduce

# COMMAND ----------

from typing import List
from pyspark.sql.functions import col,expr,lit
import time
from concurrent.futures import ThreadPoolExecutor
import json
import re
from pyspark.sql.types import Row

# COMMAND ----------

start_time = time.time()

# COMMAND ----------

log_level = logging.INFO

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

# COMMAND ----------

dbutils.widgets.text("input_json_str", """""")
dbutils.widgets.text("dr_adls_root_path","abfss://dr-uc-metastore-backup@dbkucmetastoreeusadls2dr.dfs.core.windows.net")
dbutils.widgets.text("time_travel_option", "timestamp")
dbutils.widgets.text("time_travel_value", "")

input_json_str = dbutils.widgets.get("input_json_str")
time_travel_option = dbutils.widgets.get("time_travel_option")
time_travel_value = dbutils.widgets.get("time_travel_value")

THREADS = 40
DR_ADLS_ROOT_PATH = dbutils.widgets.get("dr_adls_root_path")
BACKUP_TABLE_ADLS_PATH = '/'.join([DR_ADLS_ROOT_PATH, "backup_data"])
BATCH_JOB_TABLE_PATH = '/'.join([DR_ADLS_ROOT_PATH, "batchjob"])
BATCH_JOB_CONTROL_TABLE_PATH = '/'.join([DR_ADLS_ROOT_PATH, "batchjobcontrol"])

# COMMAND ----------

try:
    job_id = get_max_job_id(BATCH_JOB_TABLE_PATH,'Restore')
except AnalysisException as e:
    logger.error(str(e))
    job_id = 0
except Exception:
    logger.error("Unable to get max job id", exc_info=True)

logger.info("Job id of this run is %s" %job_id)

# COMMAND ----------
data = json.loads(input_json_str)

if validate_json_structure(data):
    logger.info("JSON structure is valid.")
else:
    logger.error("JSON structure is not valid")
    raise Exception("JSON structure is not valid")

# COMMAND ----------

exec(f"filter_condition={build_filter_conditions(data, BACKUP_TABLE_ADLS_PATH, spark, time_travel_option, time_travel_value)}")
logger.info("filter_condition: %s" %filter_condition)

# COMMAND ----------

# df = spark.table("cat_uc_backup.db_backup.dr_backup")
core_object_types = ["STORAGE CREDENTIAL", "EXTERNAL LOCATION","CONNECTION", "SHARE", "RECIPIENT", "CATALOG", "SCHEMA", "TABLE", "VIEW", "FUNCTION", "VOLUME", "REGISTERED MODEL"]

if time_travel_value in ("", None):
    df = spark.read.format("delta").load(BACKUP_TABLE_ADLS_PATH)
elif time_travel_option.lower() == "version":
    df = spark.read.format("delta").option("versionAsOf", time_travel_value).load(BACKUP_TABLE_ADLS_PATH)
elif time_travel_option.lower() == "timestamp":
    df = spark.read.format("delta").option("timestampAsOf", time_travel_value).load(BACKUP_TABLE_ADLS_PATH)

df_restore_with_user_filter = df.where(filter_condition)

df_owner = df.where("object_type='OWNER'").join(df_restore_with_user_filter.where(col("object_type").isin(core_object_types)).select("object_name"), ["object_name"], "inner")

df_tag = df.where("object_type='TAG'").join(df_restore_with_user_filter.where(col("object_type").isin(core_object_types)).select("object_name"), ["object_name"], "inner")

df_privilege = df.where("object_type='PRIVILEGE'").join(df_restore_with_user_filter.where(col("object_type").isin(core_object_types)).select("object_name"), ["object_name"], "inner")

df_delta_sharing_share_object = df.where("object_type='DELTA SHARING SHARE OBJECT'").join(df_restore_with_user_filter.where(col("object_type").isin(core_object_types)).select("object_name"), ["object_name"], "inner")

dfs = [df_restore_with_user_filter, df_owner, df_tag, df_privilege, df_delta_sharing_share_object]
df_restore_union = reduce(lambda df_restore_with_user_filter,df_owner: df_restore_with_user_filter.unionAll(df_owner), dfs)

df_succeeded = (spark.read.format("delta")
                .load(BATCH_JOB_CONTROL_TABLE_PATH).where(f"jobid = {job_id} and status='Succeeded'")
                .selectExpr("rowid as object_uid"))

df_restore = df_restore_union.join(df_succeeded, "object_uid", "leftanti").distinct()

# COMMAND ----------

restore_object_count = df_restore.where("object_type='TABLE'").count()
logger.info("Total Table count selected to be restored = %s" %restore_object_count)
dbutils.jobs.taskValues.set(key = 'restore_object_count', value = restore_object_count)

# COMMAND ----------

end_time = time.time()
total_time = round((end_time-start_time)/60,3)

# COMMAND ----------

dbutils.notebook.exit(f"Check restore object count Completed in {total_time} min ...")