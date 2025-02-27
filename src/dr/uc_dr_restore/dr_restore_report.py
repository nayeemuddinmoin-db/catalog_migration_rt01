# Databricks notebook source

import logging
# from utilities.send_email import *
from pepemailpkg import sendEmailGeneric
from utilities.restore_restartability_logging import *
import pandas as pd

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
dbutils.widgets.text("job_id","")
dbutils.widgets.text("send_report_to_email_id","nayeemuddin.moinuddin@databricks.com")
dbutils.widgets.text("send_report_from_email_id","Shavali.Pinjari.Contractor@pepsico.com")
dbutils.widgets.text("time_travel_option", "timestamp")
dbutils.widgets.text("time_travel_value", "")

send_report_to_email_id = dbutils.widgets.get("send_report_to_email_id")
send_report_from_email_id = dbutils.widgets.get("send_report_from_email_id")
time_travel_option = dbutils.widgets.get("time_travel_option")
time_travel_value = dbutils.widgets.get("time_travel_value")

DR_ADLS_ROOT_PATH = dbutils.widgets.get("dr_adls_root_path")
BACKUP_TABLE_ADLS_PATH = "/".join([DR_ADLS_ROOT_PATH, "backup_data"])
BATCH_JOB_TABLE_PATH =  "/".join([DR_ADLS_ROOT_PATH, "batchjob"])
BATCH_JOB_CONTROL_TABLE_PATH =  "/".join([DR_ADLS_ROOT_PATH, "batchjobcontrol"])

MAX_JOB_ID = get_max_job_id(BATCH_JOB_TABLE_PATH, 'Restore')
MAX_JOB_STATUS = get_max_job_status(BATCH_JOB_TABLE_PATH, 'Restore')

JOB_ID = MAX_JOB_ID if dbutils.widgets.get("job_id") in (None, '') else dbutils.widgets.get("job_id")
CSV_FOLDER =  "/".join([DR_ADLS_ROOT_PATH, "dr_restore_reports",str(JOB_ID)])

detailed_report_file_path = ""
status_summary_file_path = ""
restored_object_wise_count_file_path = ""

# COMMAND ----------

logger.info("Input DR_ADLS_ROOT_PATH path: %s" %DR_ADLS_ROOT_PATH)
logger.info("BACKUP_TABLE_ADLS_PATH path: %s" %BACKUP_TABLE_ADLS_PATH)
logger.info("BATCH_JOB_TABLE_PATH path: %s" %BATCH_JOB_TABLE_PATH)
logger.info("BATCH_JOB_CONTROL_TABLE_PATH path: %s" %BATCH_JOB_CONTROL_TABLE_PATH)
logger.info("MAX_JOB_ID: %s" %MAX_JOB_ID)
logger.info("MAX_JOB_STATUS: %s" %MAX_JOB_STATUS)
logger.info("JOB_ID used for this notebook execution: %s" %JOB_ID)
logger.info("CSV_FOLDER path: %s" %CSV_FOLDER)


# COMMAND ----------
if time_travel_value in ("", None):
    df_backup_table = spark.read.format("delta").load(BACKUP_TABLE_ADLS_PATH)
elif time_travel_option.lower() == "version":
    df_backup_table = spark.read.format("delta").option("versionAsOf", time_travel_value).load(BACKUP_TABLE_ADLS_PATH)
elif time_travel_option.lower() == "timestamp":
    df_backup_table = spark.read.format("delta").option("timestampAsOf", time_travel_value).load(BACKUP_TABLE_ADLS_PATH)
# df_backup_table = spark.read.format("delta").load(BACKUP_TABLE_ADLS_PATH)
df_batch_job_table = spark.read.format("delta").load(BATCH_JOB_TABLE_PATH)
df_batch_job_control_table = spark.read.format("delta").load(BATCH_JOB_CONTROL_TABLE_PATH).where(f"jobid={JOB_ID}")


df_backup_table.createOrReplaceTempView("backup_table")
df_batch_job_table.createOrReplaceTempView("batch_job_table")
df_batch_job_control_table.createOrReplaceTempView("batch_job_control_table")


# COMMAND ----------
logger.info("Creating Detailed Report DataFrame")
df_detailed_report = spark.sql("""select 
            bjc.jobid,
            bjc.rowid,
            bjc.status,
            bjc.error_message,
            bkp.object_name,
            bkp.object_type,
            bkp.object_parent_name,
            bkp.object_parent_type,
            bjc.record_timestamp,
            bkp.restore_ddl,
            bkp.api_payload,
            bkp.created_tsp
          from batch_job_control_table bjc left join backup_table bkp on bjc.rowid=bkp.object_uid""")



# COMMAND ----------
logger.info("Creating Status Summary DataFrame")
df_status_summary = spark.sql("""
with query_source as (
select 
            bjc.jobid,
            bjc.rowid,
            bjc.status,
            bkp.object_name,
            bkp.object_type,
            bkp.object_parent_name,
            bkp.object_parent_type,
            bjc.record_timestamp,
            bkp.restore_ddl,
            bkp.api_payload,
            bkp.created_tsp,
            row_number() over (partition by bjc.rowid order by record_timestamp desc) rank
          from batch_job_control_table bjc left join backup_table bkp on bjc.rowid=bkp.object_uid
)
select jobid, status, count(status) as total_count, max(record_timestamp) as record_timestamp from query_source 
where rank=1
group by jobid,status
""")

# COMMAND ----------
logger.info("Creating Object-Wise Count DataFrame")
df_restored_object_wise_count = spark.sql("""
WITH query_source AS (
    SELECT 
        bjc.jobid,
        bjc.rowid,
        bjc.status,
        bkp.object_name,
        bkp.object_type,
        bkp.object_parent_name,
        bkp.object_parent_type,
        bjc.record_timestamp,
        bkp.restore_ddl,
        bkp.api_payload,
        bkp.created_tsp,
        ROW_NUMBER() OVER (PARTITION BY bjc.rowid ORDER BY record_timestamp DESC) AS rank
    FROM batch_job_control_table bjc
    LEFT JOIN backup_table bkp ON bjc.rowid=bkp.object_uid
    -- where bjc.status='Succeeded'
)

SELECT
    jobid,
    object_type,
    object_parent_type,
    status,
    COUNT(1) AS total_count,
    max(record_timestamp) as record_timestamp
FROM query_source
WHERE rank=1
GROUP BY jobid, object_type, object_parent_type,status
ORDER BY
    CASE
        WHEN object_type='STORAGE CREDENTIAL' OR object_parent_type='STORAGE CREDENTIAL' THEN 1
        WHEN object_type='EXTERNAL LOCATION' OR object_parent_type='EXTERNAL LOCATION' THEN 2
        WHEN object_type='CONNECTION' OR object_parent_type='CONNECTION' THEN 3
        WHEN object_type='SHARE' OR object_parent_type='SHARE' THEN 4
        WHEN object_type='RECIPIENT' OR object_parent_type='RECIPIENT' THEN 5
        WHEN object_type='CATALOG' OR object_parent_type='CATALOG' THEN 6
        WHEN object_type='SCHEMA' OR object_parent_type='SCHEMA' THEN 7
        WHEN object_type='TABLE' OR object_parent_type='TABLE' THEN 8
        WHEN object_type='VIEW' OR object_parent_type='VIEW' THEN 9
        WHEN object_type='FUNCTION' OR object_parent_type='FUNCTION' THEN 10
        WHEN object_type='REGISTERED MODEL' OR object_parent_type='REGISTERED MODEL' THEN 11
        ELSE 12
    END""")


# COMMAND ----------
try:
    logger.info("Writing detailed report to this path - %s" %(CSV_FOLDER+"/detailed_report/"))
    df_detailed_report.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(CSV_FOLDER+"/detailed_report/")
    detailed_report_file_path = CSV_FOLDER+"/detailed_report/"

except Exception as e:
    logger.error("Failed to write detailed report", exc_info=True)
    raise Exception("Failed to write detailed report")


# COMMAND ----------
try:
    logger.info("Writing status summary report to this path - %s" %(CSV_FOLDER+"/status_summary/"))
    df_status_summary.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(CSV_FOLDER+"/status_summary/")
    status_summary_file_path = CSV_FOLDER+"/status_summary"

except Exception as e:
    logger.error("Failed to write status summary report", exc_info=True)
    raise Exception("Failed to write status summary report")


# COMMAND ----------
try:
    logger.info("Writing object wise count report to this path - %s" %(CSV_FOLDER+"/restored_object_wise_count/"))
    df_restored_object_wise_count.coalesce(1).write.format("csv").option("header","true").mode("overwrite").save(CSV_FOLDER+"/restored_object_wise_count/")
    restored_object_wise_count_file_path = CSV_FOLDER+"/restored_object_wise_count"

except Exception as e:
    logger.error("Failed to object wise count report", exc_info=True)
    raise Exception("Failed to object wise count report")

# COMMAND ----------
%md
####Send these Reports as Email attachements
# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/restore_report")

df_summary_report = spark.read.option("header","true").csv(status_summary_file_path)
df_object_wise_aggregated_report = spark.read.option("header","true").csv(restored_object_wise_count_file_path)
df_detailed_failure_report = spark.read.option("header","true").option("multiLine","true").csv(detailed_report_file_path).where("status='Failed'")
df_pandas_summary_report = df_summary_report.toPandas()
df_pandas_object_wise_aggregated_report = df_object_wise_aggregated_report.toPandas()
df_pandas_detailed_failure_report = df_detailed_failure_report.toPandas()
file_path_summary_report = "/dbfs/restore_report/summary_report.csv"
file_path_object_wise_aggregrated_report =  "/dbfs/restore_report/object_wise_aggregrated_report.csv"
file_path_detailed_failure_report = "/dbfs/restore_report/detailed_failure_report.csv"
file_path_objects_to_be_restored = "/dbfs/restore_report/objects_selected_to_be_restored.csv"

df_pandas_summary_report.to_csv(file_path_summary_report, header=True, sep=',', index=False)
df_pandas_object_wise_aggregated_report.to_csv(file_path_object_wise_aggregrated_report, header=True, sep=',', index=False)
df_pandas_detailed_failure_report.to_csv(file_path_detailed_failure_report, header=True, sep=',', index=False)
df_pandas_objects_to_be_restored = pd.read_csv(file_path_objects_to_be_restored)

# Convert DataFrame to HTML table
html_table_objects_to_be_restored = df_pandas_objects_to_be_restored.to_html(index=False)

# COMMAND ----------
workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")
reports_files = f"{file_path_summary_report},{file_path_object_wise_aggregrated_report},{file_path_detailed_failure_report}"
reports_files

# COMMAND ----------

mail_body = f"""
<html>
                <head>
                    <style>
                        table {{
                            border-collapse: collapse;
                            width: 100%;
                        }}
                        th, td {{
                            border: 1px solid black;
                            padding: 8px;
                            text-align: left;
                        }}
                        th {{
                            background-color: #FFBF00; /* Amber color */
                        }}
                    </style>
                </head>
                <body>
                    
                    <h2> Objects Selected to be Restored </h2>
                    <h4> Workspace: {workspace_url} </4>
                    {html_table_objects_to_be_restored}
                    <h2>Restore Report - JobId - {JOB_ID}</h2>
                    <table>
                        <tr>
                            <th>Report Name</th>
                            <th>ADLS Download Path</th>
                        </tr>
                        <tr>
                            <td>Summary Report</td>
                            <td>{status_summary_file_path}</td>
                        </tr>
                        <tr>
                            <td>Object-wise Aggregated Report</td>
                            <td>{restored_object_wise_count_file_path}</td>
                        </tr>
                        <tr>
                            <td>Detailed Report</td>
                            <td>{detailed_report_file_path}</td>
                        </tr>
                    </table>
                </body>
                </html>

"""

# COMMAND ----------
try:
    sendEmailGeneric(secret=dbutils.secrets.get('edapemail','email-secret'), 
                 emailfrom=send_report_from_email_id,
                 subject=f"UC DR Restore Report",
                 message=mail_body,
                 receiver=send_report_to_email_id,
                 attachments=reports_files,
                 client_id=None,
                 scope=None,
                 authority=None)
except Exception as e:
    logger.error("Failed to send email: ", exc_info=True)

# COMMAND ----------
%md
### Detailed Report
# COMMAND ----------
df_detailed_report.display()

# COMMAND ----------
%md
### Restored Object wise count
# COMMAND ----------
df_restored_object_wise_count.display()

# COMMAND ----------
%md
### Status Summary Report
# COMMAND ----------
df_status_summary.display()

# COMMAND ----------
dbutils.notebook.exit("Completed generating Restore reports")