# Databricks notebook source

from utilities.restore_restartability_logging import * 
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

dbutils.widgets.text("pre_restore_version","")
dbutils.widgets.text("post_restore_version","")
dbutils.widgets.text("dr_adls_root_path","abfss://dr-uc-metastore-backup@dbkucmetastoreeusadls2dr.dfs.core.windows.net")

# COMMAND ----------

pre_restore_version = dbutils.widgets.get("pre_restore_version")
post_restore_version = dbutils.widgets.get("post_restore_version")
DR_ADLS_ROOT_PATH = dbutils.widgets.get("dr_adls_root_path")

# COMMAND ----------

max_job_id = get_max_job_id(batch_table_path='/'.join([DR_ADLS_ROOT_PATH,'batchjob']), job_type='Restore')

# COMMAND ----------

logger.info("DR_ADLS_ROOT_PATH: %s" %DR_ADLS_ROOT_PATH)
logger.info("pre_restore_version: %s" %pre_restore_version)
logger.info("post_restore_version: %s" %post_restore_version)
logger.info("max_job_id: %s" %max_job_id)

# COMMAND ----------

if pre_restore_version == "" or pre_restore_version is None:
    pre_restore_version = spark.sql(f"select backupversion from delta.`{DR_ADLS_ROOT_PATH}/batchjob`").first()[0]
    logger.info("pre_restore_version: %s" %pre_restore_version)

# COMMAND ----------

if post_restore_version == "" or post_restore_version is None:
    post_restore_version = spark.sql(f"select backupversion from delta.`{DR_ADLS_ROOT_PATH}/batchjob`").first()[0]
    logger.info("post_restore_version: %s" %post_restore_version)

# COMMAND ----------

if pre_restore_version == post_restore_version:
    logger.error("Check Pre/Post Restore backup table version")
    raise Exception("Run Backup job to get latest snapshot of the backup table")

# COMMAND ----------

spark.sql(f"""
(
  (
    select
      'Post' as backup_source,
      object_name,
      object_type,
      object_parent_name,
      object_parent_type,
      object_cross_relationship,
      object_uid,
      restore_ddl,
      api_payload
    from
      delta.`{DR_ADLS_ROOT_PATH}/backup_data` version as of {post_restore_version}
  )
  except
    (
      select
        'Post' as backup_source,
        object_name,
        object_type,
        object_parent_name,
        object_parent_type,
        object_cross_relationship,
        object_uid,
        restore_ddl,
        api_payload
      from
        delta.`{DR_ADLS_ROOT_PATH}/backup_data` version as of {pre_restore_version}
    )
)
union all
  (
    (
      select
        'Pre' as backup_source,
        object_name,
        object_type,
        object_parent_name,
        object_parent_type,
        object_cross_relationship,
        object_uid,
        restore_ddl,
        api_payload
      from
        delta.`{DR_ADLS_ROOT_PATH}/backup_data` version as of {pre_restore_version}
    )
    except
      (
        select
          'Pre' as backup_source,
          object_name,
          object_type,
          object_parent_name,
          object_parent_type,
          object_cross_relationship,
          object_uid,
          restore_ddl,
          api_payload
        from
          delta.`{DR_ADLS_ROOT_PATH}/backup_data` version as of {post_restore_version}
      )
  )
  """).display()

# COMMAND ----------

dbutils.notebook.exit("pre post check completed ...")
