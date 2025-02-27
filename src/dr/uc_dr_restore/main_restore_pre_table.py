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
NUM_OF_SCHEMA_GROUPS = 8
GROUPED_SCHEMAS_PATH = '/'.join([DR_ADLS_ROOT_PATH, "grouped_schemas"])

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
logger.info("Count of Objects Selected to be Restored")
df_restore.groupBy(col("object_type")).count().show()
# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/restore_report")
df_pandas_objects_to_be_restored = df_restore.groupBy(col("object_type")).count().toPandas()
file_path_objects_to_be_restored = "/dbfs/restore_report/objects_selected_to_be_restored.csv"
df_pandas_objects_to_be_restored.to_csv(file_path_objects_to_be_restored, header=True, sep=',', index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Restore UC objects from backup

# COMMAND ----------
# CREATE STORAGE CREDENTIAL

restore_manager_storage_credential = RestoreManagerStorageCredential(logger)
storage_cred_list_create=restore_manager_storage_credential.get_storage_credential(logger, df_restore)

if len(storage_cred_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        catalog_restore_result = exe.map(restore_manager_storage_credential.create_storage_credential,storage_cred_list_create)
else:
    logger.info("No storage credential to create")

# Storage Credential Creation Checkpoint.

logger.info("updating the checkpoint table BatchJobControl with STORAGE CREDENTIAL objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_storage_credential.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='STORAGE CREDENTIAL'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_storage_credential.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with STORAGE CREDENTIAL objects", exc_info=True)


# Granting the restore executor all the required permission on storage credential it needs to restore uc objects

logger.info("Granting permissions on STORAGE CREDENTIAL to restore objects dependent on it")
restore_manager_storage_credential_pre_step = RestoreManagerStorageCredential(logger)
st_credentials_list_create_pre_step=restore_manager_storage_credential_pre_step.get_storage_credential(logger, df)

if len(st_credentials_list_create_pre_step) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        result = exe.map(restore_manager_storage_credential_pre_step.grant_restore_executor_permission_pre_step,st_credentials_list_create_pre_step)
else:
    logger.info("No storage credential found in backup table")

# COMMAND ----------
# CREATE EXTERNAL LOCATIONS 

restore_manager_external_location = RestoreManagerExternalLocation(logger)
ext_locations_list_create=restore_manager_external_location.get_external_locations(logger, df_restore)

if len(ext_locations_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        catalog_restore_result = exe.map(restore_manager_external_location.create_external_locations,ext_locations_list_create)
else:
    logger.info("No external location to create")

# External Location Creation Checkpoint

logger.info("updating the checkpoint table BatchJobControl with EXTERNAL_LOCATION objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_external_location.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='EXTERNAL LOCATION'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_external_location.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with EXTERNAL_LOCATION objects", exc_info=True)


# Granting the restore executor all the required permission on external location it needs to restore uc objects

logger.info("Granting permissions on EXTERNAL LOCATION to restore objects dependent on it")
restore_manager_external_location_pre_step = RestoreManagerExternalLocation(logger)
ext_locations_list_create_pre_step=restore_manager_external_location_pre_step.get_external_locations(logger, df)

if len(ext_locations_list_create_pre_step) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        result = exe.map(restore_manager_external_location_pre_step.grant_restore_executor_permission_pre_step,ext_locations_list_create_pre_step)
else:
    logger.info("No external location found in backup table")

# COMMAND ----------
# CREATE CONNECTIONS 

restore_manager_connection = RestoreManagerConnection(logger)
connections_list_create=restore_manager_connection.get_connections(logger, df_restore)

if len(connections_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        connection_restore_result = exe.map(restore_manager_connection.create_uc_object,connections_list_create)
else:
    logger.info("No connection to create")

# Connection Creation Checkpoint

logger.info("updating the checkpoint table BatchJobControl with CONNECTION objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_connection.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='CONNECTION'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_connection.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with CONNECTION objects", exc_info=True)

# COMMAND ----------
# CREATE CATALOG 

restore_manager_catalog = RestoreManagerCatalog(logger)
catalogs_list_create=restore_manager_catalog.get_catalogs(logger, df_restore)

if len(catalogs_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        catalog_restore_result = exe.map(restore_manager_catalog.create_uc_object,catalogs_list_create)
else:
    logger.info("No catalog to create")

# Catalog Creation Checkpoint

logger.info("updating the checkpoint table BatchJobControl with CATALOG objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_catalog.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='CATALOG'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_catalog.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with CATALOG objects", exc_info=True)

# Granting the restore executor all the required permission on catalog it needs to restore uc objects

logger.info("Granting permissions on CATALOG to restore objects dependent on it")
restore_manager_catalog_pre_step = RestoreManagerCatalog(logger)
catalogs_list_create_pre_step=restore_manager_catalog_pre_step.get_catalogs(logger, df)

if len(catalogs_list_create_pre_step) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        result = exe.map(restore_manager_catalog_pre_step.grant_restore_executor_permission_pre_step,catalogs_list_create_pre_step)
else:
    logger.info("No catalog found")

# COMMAND ----------
# CREATE SCHEMA

restore_manager_schema = RestoreManagerSchema(logger)
schemas_list_create=restore_manager_schema.get_schemas(logger, df_restore)

if len(schemas_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        schema_restore_result = exe.map(restore_manager_schema.create_uc_object,schemas_list_create)
else:
    logger.info("No schema to create")


# Schema Creation Checkpoint

logger.info("updating the checkpoint table BatchJobControl with SCHEMA objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_schema.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='SCHEMA'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_schema.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with SCHEMA objects", exc_info=True)

# COMMAND ----------
# CREATE VOLUME

restore_manager_volume = RestoreManagerVolume(logger)
volumes_list_create=restore_manager_volume.get_volumes(logger, df_restore)

if len(volumes_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        table_restore_result = exe.map(restore_manager_volume.create_uc_object,volumes_list_create)
else:
    logger.info("No Volumes to create")


# Volume Creation Checkpoint

logger.info("updating the checkpoint table BatchJobControl with VOLUME objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_volume.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='VOLUME'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_volume.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with VOLUME objects", exc_info=True)

# COMMAND ----------

# CREATE REGISTERED MODEL

restore_manager_registered_model = RestoreManagerRegisteredModel(logger)
registered_model_list_create=restore_manager_registered_model.get_registered_model(logger, df_restore)

if len(registered_model_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        reg_model_restore_result = exe.map(restore_manager_registered_model.create_registered_model,registered_model_list_create)
else:
    logger.info("No Registered models to create")


# Registered Model creation checkpoint

logger.info("updating the checkpoint table BatchJobControl with REGISTERED MODEL objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_registered_model.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='REGISTERED MODEL'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_registered_model.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with REGISTERED MODEL objects", exc_info=True)

# COMMAND ----------

# CREATE SHARE

restore_manager_share = RestoreManagerShare(logger)
shares_list_create=restore_manager_share.get_shares(logger, df_restore)

if len(shares_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        table_restore_result = exe.map(restore_manager_share.create_uc_object,shares_list_create)
else:
    logger.info("No Shares to create")


# Share creation checkpoint

logger.info("updating the checkpoint table BatchJobControl with SHARE objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_share.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='SHARE'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_share.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with SHARE objects", exc_info=True)

# Granting the restore executor ownership on share which needs to restore uc objects

logger.info("Granting ownership on SHARE to restore objects dependent on it")
restore_manager_share_pre_step = RestoreManagerShare(logger)
shares_list_create_pre_step=restore_manager_share_pre_step.get_shares(logger, df_restore)

if len(shares_list_create_pre_step) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        result = exe.map(restore_manager_share_pre_step.grant_restore_executor_ownership_pre_step,shares_list_create_pre_step)
else:
    logger.info("No share found")

# COMMAND ----------
# CREATE RECIPIENT

restore_manager_recipient = RestoreManagerRecipient(logger)
recipients_list_create=restore_manager_recipient.get_recipients(logger, df_restore)

if len(recipients_list_create) > 0:
    with ThreadPoolExecutor(max_workers=THREADS) as exe:
        table_restore_result = exe.map(restore_manager_recipient.create_recipients,recipients_list_create)
else:
    logger.info("No Recipients to create")


# Recipient creation checkpoint

logger.info("updating the checkpoint table BatchJobControl with RECIPIENT objects ...")
try:
    failed_schema = "object_uid LONG, error_message STRING"
    df_failed = spark.createDataFrame(restore_manager_recipient.uc_object_failed_list, schema=failed_schema)
    insert_batch_job_control_execution(df_restore.where("object_type='RECIPIENT'").select("object_uid").where(~col("object_uid").isin([x[0] for x in restore_manager_recipient.uc_object_failed_list])).withColumn("error_message",lit(None)).withColumn("status",lit("Succeeded")).unionAll(df_failed.withColumn("status",lit("Failed"))), BATCH_JOB_CONTROL_TABLE_PATH, job_id)
except Exception as e:
    logger.error("Failed to update the checkpoint table BatchJobControl with RECIPIENT objects", exc_info=True)

# COMMAND ----------

df_restore_with_user_filter.where("object_type='TABLE'").createOrReplaceTempView("tbl_selected_to_be_restored")
df_grouped_schemas = spark.sql(f"""
               with src as (
                    select object_parent_name,
                    count(1) as object_count 
                    from tbl_selected_to_be_restored
                    group by object_parent_name),
                ranked_schemas as (
                select *,
                mod(row_number() over(order by object_count desc),{NUM_OF_SCHEMA_GROUPS}) as group_id
                from src
                )
                select * from ranked_schemas
            """)

# COMMAND ----------
logger.info("Table Schemas and their Group Ids:")
df_grouped_schemas.display()

# COMMAND ----------

try:
    df_grouped_schemas.write.format("delta").mode("overwrite").save(GROUPED_SCHEMAS_PATH)
except Exception as e:
    logger.info("Failed writing grouped schema data")
    raise Exception("Failed writing grouped schema data")

# COMMAND ----------

end_time = time.time()
total_time = round((end_time-start_time)/60,3)

# COMMAND ----------

dbutils.notebook.exit(f"Object Restore Completed in {total_time} min ...")