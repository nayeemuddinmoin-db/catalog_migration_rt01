import logging
import subprocess
import re
import datetime as dt
import dr.utils.send_email as send_email

from pyspark.sql import SparkSession, Row, functions as F, window as W, DataFrame
from pyspark.dbutils import DBUtils
from concurrent.futures import ThreadPoolExecutor
from typing import Tuple, List
from databricks.sdk import WorkspaceClient


THREADS = 8
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def uc_dr_pre_checks(backup_table_path: str = None, token_scope: str = None) -> None:
    """
    Pre Disaster Recovery checks. It will check whether the 
    External Locations have access to the storage accounts.

    Args:
        backup_table_path (str): The path of the backup table
        token_scope (str): The scope of the token
    """

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    logger.info("Running pre Disaster Recovery Backup checks")
    # bind_catalogs(backup_table_path, token_scope)
    logger.info("Pre Disaster Recovery Backup checks completed")
    

def uc_dr_post_checks(df_ret: DataFrame, table_storage_location: str, email_credential_secret_scope: str) -> None:
    """
    Post Disaster Recovery checks. It will check whether the
    data was successfully restored.

    Args:
        table_storage_location (str): The table storage location
        email_credential_secret_scope (str): The email credential secret scope
    """

    df = spark.read.format("delta").load(table_storage_location)

    logger.info("Running post Disaster Recovery Backup checks")
    
    logger.info("--------------------")
    logger.info("DR Post-Check: Empty Restore DDL and API Payload")

    try:
        ds_catalogs = df_ret.filter(F.col("restore_ddl").like("%USING SHARE%") & (F.col("object_type")==F.lit("CATALOG"))).select("object_name").collect()
        ds_catalogs = [row.object_name for row in ds_catalogs]

        # remove delta sharing catalog list of empty restore ddl and api payload
        isEmpty = (
            df_ret.withColumn("catalog", F.regexp_extract(F.col("object_name"), "^([^\.]+)\.", 1))
            .filter(~F.col("catalog").isin(ds_catalogs))
            .filter("restore_ddl is null and api_payload is null")
            .isEmpty()
        )

        if not isEmpty:
            if email_credential_secret_scope not in ("", None):
                logger.error("Error: Empty Restore DDL and API Payload")
                try:
                    workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

                    send_email.sendEmailGeneric(
                        credential_scope=email_credential_secret_scope,
                        emailfrom=send_email.EMAIL_TEMPLATES["BACKUP_FAILURE"]["from"],
                        receiver=send_email.EMAIL_TEMPLATES["BACKUP_FAILURE"]["to"],
                        subject=send_email.EMAIL_TEMPLATES["BACKUP_FAILURE"]["subject"],
                        message=send_email.EMAIL_TEMPLATES["BACKUP_FAILURE"]["message"].format(workspace_url=workspace_url))
                except Exception as e:
                    logger.error(f"Error sending email: {e}")
            else:
                logger.error("Error: No available credentials to send email")
                raise ValueError("No available credentials to send email")
        else:
            logger.info("DR Post-Check: Empty Restore DDL and API Payload - finished")
    except Exception as e:
        logger.error(f"Error checking Empty Restore DDL and API Payload: {e}")

    logger.info("\n--------------------")
    logger.info("DR Post-Check: Backup Summary")
    stats_df = df.groupBy("object_type").count().orderBy("object_type")

    if email_credential_secret_scope not in ("", None) and not stats_df.isEmpty():
        try:

            tbl_details = (
                spark.sql(f"DESCRIBE HISTORY delta.`{table_storage_location}`")
                .orderBy(F.col("version").desc())
                .limit(1)
                .select("version", "timestamp")
            ).collect()[0]

            workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
            stats_df_html = stats_df.toPandas().to_html(index=False, justify="center")

            send_email.sendEmailGeneric(
                credential_scope=email_credential_secret_scope,
                emailfrom=send_email.EMAIL_TEMPLATES["BACKUP_STATS"]["from"],
                receiver=send_email.EMAIL_TEMPLATES["BACKUP_STATS"]["to"],
                subject=send_email.EMAIL_TEMPLATES["BACKUP_STATS"]["subject"].format(workspace_url=workspace_url),
                message=send_email.EMAIL_TEMPLATES["BACKUP_STATS"]["message"].format(html_table=stats_df_html, 
                                                                                     workspace_url=workspace_url, 
                                                                                     backup_table_version=tbl_details.version, 
                                                                                     backup_table_timestamp=tbl_details.timestamp))
        except Exception as e:
            logger.error(f"Error sending email: {e}")

    stats_df.show(truncate=False)


def test_storage_account_connectivity() -> Tuple[str, str]:
    """
    Test the connectivity to the storage account

    Returns:
        Tuple[str, str]: The external location and the result
    """

    logger.info("--------------------")
    logger.info("DR Pre-Check: Check Azure Storage Accounts connectivity")

    external_locations_list = (
        spark.table("system.information_schema.external_locations")
        .filter("external_location_name is not null and url is not null")
        .select("external_location_name", "url")
        .withColumn("host", F.split(F.split("url", "@")[1], "/")[0])
        .groupBy("host")
        .agg(F.collect_list("external_location_name").alias("external_location_names"))
        .collect()
    )

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        result = [result for result in executor.map(storage_account_connectivity, external_locations_list)]

    logger.info("DR Pre-Check: Check Azure Storage Accounts connectivity - finished")

    return result

def storage_account_connectivity(external_location: Row) -> Tuple[str, str, str]:
    """
    Test the connectivity to the storage account

    Args:
        external_location (Row): The external location

    Returns:
        Tuple[str, str]: The external location and the result
    """

    # Cleanup the endpoint
    # host = external_location.url.split("@")[1].split("/")[0]
    command = f"nc -zv {external_location.host} 443"

    status = "error"
    try:
        subprocess.run(command, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
        status = "success"
    except subprocess.CalledProcessError as e:
        logger.error(f"Error testing the connectivity to {external_location.external_location_names}: {e}")
    except subprocess.TimeoutExpired:
        logger.error(f"Timeout testing the connectivity to {external_location.external_location_names}")

    return (external_location.external_location_names, external_location.host, status)

def bind_catalogs(backup_table_path: str = None, token_scope: str = None) -> None:
    """
    Bind the catalogs to the workspace

    Args:
        backup_table_path (str): The path of the backup table
        token_scope (str): The scope of the token
    """
    logger.info("--------------------")
    logger.info("DR Pre-Check: Binding Catalogs to Workspace")
    
    wind = W.Window.partitionBy("catalog_name").orderBy(F.col("event_time").desc())

    try:
        workspace_url = spark.conf.get("spark.databricks.workspaceUrl")
        metastore_id = spark.sql("select current_metastore()").collect()[0][0].split(":")[-1]

        all_catalogs = (
            spark.table("system.access.audit")
            .filter(F.col("event_time") > F.lit(get_audit_log_time_range(backup_table_path)))
            .filter("action_name in ('createCatalog', 'deleteCatalog', 'getCatalog', 'updateCatalog', 'listCatalog')")
            .filter(f"request_params.metastore_id = '{metastore_id}'")
            # TODO: check listCatalog parsing
            .withColumn("catalog_name", F.when(F.col("action_name") == F.lit("createCatalog"), F.col("request_params.name"))
                                        .when(F.col("action_name").isin(["deleteCatalog", "getCatalog", "updateCatalog", "listCatalog"]), F.col("request_params.name_arg"))
                                        .otherwise(F.col("request_params.catalog_name")))
            .select("catalog_name", "action_name", "event_time")
            .withColumn("row_number", F.row_number().over(wind))
            .filter("row_number = 1 and action_name <> 'deleteCatalog' and catalog_name not in ('system', 'default', 'hive_metastore', 'spark_catalog', '__databricks_internal')")
            .select("catalog_name")
        )

        bound_catalogs = spark.table("system.information_schema.catalogs").select("catalog_name")

        to_bind_catalogs_df = all_catalogs.subtract(bound_catalogs)

    except Exception as e:
        logger.error(f"Error reading catalogs. No catalog will be bound to the current workpace. Error: {e}")
        return

    if to_bind_catalogs_df.isEmpty():
        logger.info("All catalogs are bound to the workspace")
        return
    
    to_bind_catalogs = to_bind_catalogs_df.collect()
    logger.info(f"The following catalogs are not bound to the workspace: {to_bind_catalogs}")

    try:
        token = dbutils.secrets.get(scope=token_scope, key="databricks-token")
    except Exception as e:
        logger.error(f"Error reading Databricks token. No catalog will be bound to the current workpace. Error: {e}")
        return
    
    w = WorkspaceClient(host=workspace_url, token=token)
    
    pattern = re.compile(r"^(?:https:\/\/)?adb-([\d]+)\.\d\.azuredatabricks\.net$")
    if pattern.match(workspace_url):
        workspace_id = pattern.match(workspace_url).group(1)
    else:
        logger.error(f"Workspace URL {workspace_url} is not in the expected format")
        return

    for catalog in to_bind_catalogs:
        try:
            # w.workspace_bindings.update(name=catalog.catalog_name, assign_workspaces=[workspace_id])
            logger.info(f"Bound catalog {catalog.catalog_name} to workspace {workspace_id}")
        except Exception as e:
            if f"catalog '{catalog.catalog_name}' does not exist" in str(e).lower():
                logger.info(f"Catalog '{catalog.catalog_name}' does not exist")
            else:
                logger.error(f"Error binding catalog {catalog.catalog_name} to workspace {workspace_id}. Error: {e}")
        
    logger.info("DR Pre-Check: Bind Catalogs to Workspace - finished")

def get_audit_log_time_range(backup_table_path) -> dt.datetime:
    """
    Get the time range of the audit log for the given backup table path

    Args:
        backup_table_path (str): The path of the backup table

    Returns:
        dt.datetime: The time range of the audit log
    """

    try:
        latest_backup_run = (
            spark.read.format("delta").load(backup_table_path)
            .select("max(created_tsp) as created_tsp")
            .collect()[0][0]
        )
        
        time_range = latest_backup_run - dt.timedelta(hours=2)
        logger.info(f"Reading audit logs since: {time_range}")
    except:
        logger.info(f"Backup table is empty. Should read all available audit logs to bind catalogs")
        return dt.datetime(1900, 1, 1)

    return time_range