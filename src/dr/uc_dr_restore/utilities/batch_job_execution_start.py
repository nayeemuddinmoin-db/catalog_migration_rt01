# Databricks notebook source

from restore_restartability_logging import *
import logging

# COMMAND ----------

log_level = logging.INFO

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

# COMMAND ----------

dbutils.widgets.text("dr_adls_root_path","abfss://dr-uc-metastore-backup@dbkucmetastoreeusadls2dr.dfs.core.windows.net")
dbutils.widgets.text("restart_mode", "FRESH_START")

dbutils.widgets.text("time_travel_option", "timestamp")
dbutils.widgets.text("time_travel_value", "")

time_travel_option = dbutils.widgets.get("time_travel_option")
time_travel_value = dbutils.widgets.get("time_travel_value")

restart_mode = dbutils.widgets.get("restart_mode")
DR_ADLS_ROOT_PATH = dbutils.widgets.get("dr_adls_root_path")

BATCH_JOB_TABLE_PATH = '/'.join([DR_ADLS_ROOT_PATH, 'batchjob'])
BATCH_JOB_CONTROL_TABLE_PATH = '/'.join([DR_ADLS_ROOT_PATH, 'batchjobcontrol'])

# COMMAND ----------

if time_travel_value == "" or None:
    backup_table_version = get_latest_table_version(f'{DR_ADLS_ROOT_PATH}/backup_data')
elif time_travel_option == 'version':
    backup_table_version = time_travel_value
elif time_travel_option == 'timestamp':
    backup_table_version = get_table_version_from_timestamp(f'{DR_ADLS_ROOT_PATH}/backup_data',time_travel_value)  

# COMMAND ----------

try:
    spark.read.format("delta").load(BATCH_JOB_TABLE_PATH)
except AnalysisException as e:
    logger.error("Path not found error")
    logger.error("Initializing BatchJob and BatchJobControl delta path ")
    batchjob_schema = "jobid long, jobtype string, jobstatus string, jobstart timestamp, jobend timestamp, backupversion int"
    df_batchjob = spark.createDataFrame([], schema=batchjob_schema)
    df_batchjob.write.mode("append").format("delta").save(BATCH_JOB_TABLE_PATH)

    batchjobcontrol_schema = "jobid long, rowid long, status string, error_message string, record_timestamp timestamp"
    df_batchjobcontrol = spark.createDataFrame([], schema=batchjobcontrol_schema)
    df_batchjobcontrol.write.mode("append").format("delta").save(BATCH_JOB_CONTROL_TABLE_PATH)
    spark.sql(f"ALTER TABLE delta.`{BATCH_JOB_CONTROL_TABLE_PATH}` SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")

except Exception as e:
    # Handle other AnalysisExceptions
    logger.error("Unable to read path", exc_info=True)


if restart_mode == "FRESH_START":
    logger.info("Running the job in %s mode" %restart_mode)
    try:
        update_batch_execution_status(BATCH_JOB_TABLE_PATH,'Restore')
        insert_batch_execution(BATCH_JOB_TABLE_PATH, 'Restore', 'Running', backup_table_version)
    except Exception as e:
        error_message = f"Batch Job Execution update status or record new execution has failed with error - {e}"
        raise Exception(error_message)
else:
    logger.info("Running the job in %s mode" %restart_mode)
    try:
        if get_max_job_id(BATCH_JOB_TABLE_PATH,'Restore') ==0:
            insert_batch_execution(BATCH_JOB_TABLE_PATH, 'Restore', 'Running', backup_table_version)


        if get_max_job_status(BATCH_JOB_TABLE_PATH,'Restore') in ('Succeeded','Completed'):
            insert_batch_execution(BATCH_JOB_TABLE_PATH, 'Restore', 'Running', backup_table_version)
        else:
            update_batch_execution_status(BATCH_JOB_TABLE_PATH,'Restore', 'Failed', 'Running', None)
    except Exception as e:
        error_message = f"Batch Job Execution update status or record new execution has failed with error - {e}"
        raise Exception(error_message)

# COMMAND ----------

dbutils.notebook.exit("Bach Job Started ...")