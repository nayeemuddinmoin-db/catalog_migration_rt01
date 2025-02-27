# Databricks notebook source

from restore_restartability_logging import *

# COMMAND ----------

dbutils.widgets.text("Job_Status", "Succeeded")
dbutils.widgets.text("dr_adls_root_path", "abfss://dr-uc-metastore-backup@dbkucmetastoreeusadls2dr.dfs.core.windows.net")

job_status = dbutils.widgets.get("Job_Status")
DR_ADLS_ROOT_PATH = dbutils.widgets.get("dr_adls_root_path")
BATCH_JOB_TABLE_PATH = '/'.join([DR_ADLS_ROOT_PATH, 'batchjob'])


# COMMAND ----------

try:
    update_batch_execution_status(BATCH_JOB_TABLE_PATH,'Restore', 'Running', f'{job_status}', datetime.now())
except Exception as e:
        error_message = f"Batch Job Execution update status has failed with error - {e}"
        raise Exception(error_message)

# COMMAND ----------

dbutils.notebook.exit("Bach Job Completed ...")