# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ##### Project Name: UC Metastore Metadata DR
# MAGIC
# MAGIC ##### Purpose: Storage Account Connectivity Pro Active Monitoring
# MAGIC
# MAGIC ##### Note:
# MAGIC - This notebook will list all UC Metastore External Locations and check if the connectivity is established
# MAGIC - If the connectivity is not established:
# MAGIC   - It will create a SNOW ticket for PE creation, if none exists, and email EDAP team
# MAGIC   - If PE exists, it will email EDAP team
# MAGIC
# MAGIC - Requires:
# MAGIC   - azure-identity==1.16.0
# MAGIC   - azure-mgmt-network==25.3.0
# MAGIC   - azure-mgmt-storage==21.1.0
# MAGIC   - azure-mgmt-subscription==3.1.1
# MAGIC   - msal==1.28.0
# MAGIC   - custom sendemail wheel package from PepsiCo
# MAGIC 
# MAGIC ##### Usage:
# MAGIC - This notebook is meant to be run periodically to monitor SA connectivity
# MAGIC - Input parameters:
# MAGIC   - `azure_creds`(required): Name of the Azure credentials
# MAGIC   - `pe_subs_id`(required): Subscription ID of the private endpoint
# MAGIC   - `pe_rg_name`(required): Resource Group name of the private endpoint
# MAGIC   - `pe_vnet_name`(required): Virtual Network name of the private endpoint
# MAGIC   - `pe_subnet_name`(required): Subnet name of the private endpoint
# MAGIC   - `email_credential_scope`(optional): Email Credential Databricks Secret Scope (expects a key named `email-secret` inside of this scope)
# MAGIC   
# MAGIC ##### Revision History:
# MAGIC
# MAGIC | Date | Author | Description |
# MAGIC |---------|----------|---------------|
# MAGIC |04/12/2024| Wagner Silveira - Databricks | Initial Development |


# COMMAND ----------
import pathlib
import sys
import logging
import datetime as dt

from typing import List, Tuple

start_time = dt.datetime.now()

path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
path = "/Workspace" + path if not path.startswith("/Workspace") else path

modules_path = pathlib.Path(path).joinpath("../../../src").resolve()

# Allow for execution outside of Databricks Repos directory
sys.path.append(str(modules_path))

import dr.utils.uc_dr_checks as uc_dr_checks
import dr.utils.service_now as service_now
import dr.utils.azure_read as azure_read
import dr.utils.send_email as send_email

# COMMAND ----------
logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# COMMAND ----------
dbutils.widgets.text("azure_creds", "", "Azure Credentials")
dbutils.widgets.text("pe_subs_id", "", "Private Endpoint Subscription ID")
dbutils.widgets.text("pe_rg_name", "", "Private Endpoint Resource Group Name")
dbutils.widgets.text("pe_vnet_name", "", "Private Endpoint VNet Name")
dbutils.widgets.text("pe_subnet_name", "", "Private Endpoint Subnet Name")
dbutils.widgets.text("email_credential_scope", "edapemail", "Email Credential Databricks Secret Scope")
dbutils.widgets.text("emailfrom", "Shavali.Pinjari.Contractor@pepsico.com", "Email From")
dbutils.widgets.text("emailto", "naeem.akhtar.contractor@pepsico.com;rakesh.kundarapu.contractor@pepsico.com;josephraymond.tena.contractor@pepsico.com;ravi.munaganuri@pepsico.com", "Email To")

azure_creds = dbutils.widgets.get("azure_creds").strip()
pe_subs_id = dbutils.widgets.get("pe_subs_id").strip()
pe_rg_name = dbutils.widgets.get("pe_rg_name").strip()
pe_vnet_name = dbutils.widgets.get("pe_vnet_name").strip()
pe_subnet_name = dbutils.widgets.get("pe_subnet_name").strip()
email_credential_scope = dbutils.widgets.get("email_credential_scope").strip()
emailfrom = dbutils.widgets.get("emailfrom").strip()
emailto = dbutils.widgets.get("emailto").strip()

# if "" in (azure_creds, pe_subs_id, pe_rg_name, pe_vnet_name, pe_subnet_name, email_credential_scope):
if "" in (email_credential_scope, emailfrom, emailto):
    e = "Please provide all the required parameters"
    logger.error(e)
    raise ValueError(e)

# COMMAND ----------
def list_to_html_table(sas: List) -> str:
    """
    Converts a list of lists to an HTML table

    Args:
        sas (List): List of Storage Accounts without connectivity

    Returns:
        str: HTML table
    """

    html_table = """<table border='1'>
  <thead>
    <tr>
      <th>DBX External Locations</th>
      <th>Storage Account Host</th>
      <th>Connectivity Status</th>
    </tr>
  </thead>"""
    
    for row in sas:
        html_table += "<tr>"
        for col in row:
            if isinstance(col, list):
                html_table += "<td style='text-align:center; vertical-align:middle'>{}</td>".format(", ".join(col))
            else:
                html_table += "<td style='text-align:center; vertical-align:middle'>{}</td>".format(col)
        html_table += "</tr>"
    html_table += "</table>"
    return html_table

# COMMAND ----------
logger.info("Running Disaster Recovery checks for Storage Accounts Connectivity")
storage_accounts = uc_dr_checks.test_storage_account_connectivity()

failed_storage_accounts = [storage_accounts[i] for i in range(len(storage_accounts)) if storage_accounts[i][2] == "error"]
successful_storage_accounts = [storage_accounts[i] for i in range(len(storage_accounts)) if storage_accounts[i][2] == "success"]

# COMMAND ----------
logger.info(f"Storage Accounts with connectivity: {len(successful_storage_accounts)}; without connectivity: {len(failed_storage_accounts)}")

if failed_storage_accounts:
    try:
        workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

        send_email.sendEmailGeneric(
            credential_scope=email_credential_scope,
            emailfrom=emailfrom,
            receiver=emailto,
            subject=send_email.EMAIL_TEMPLATES["SA_WITHOUT_CONN"]["subject"],
            message=send_email.EMAIL_TEMPLATES["SA_WITHOUT_CONN"]["message"].format(workspace_url=workspace_url, 
                                                                                    html_table=list_to_html_table(failed_storage_accounts)))
    except Exception as e:
        logger.error(f"Error sending email: {e}")
        raise e

# COMMAND ----------

# for storage_account in storage_accounts:
    
#     if azure_read.check_existing_pe(azure_creds=azure_creds,
#                                     sa_name=storage_account['name'],
#                                     pe_subs_id=pe_subs_id,
#                                     pe_rg_name=pe_rg_name,
#                                     pe_vnet_name=pe_vnet_name,
#                                     pe_subnet_name=pe_subnet_name):

#         logger.info("Private Endpoint already exists. No SNOW ticket will be created for this connection. Stopping..")

#         try:
#             workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

#             send_email.sendEmailGeneric(
#                 credential_scope=email_credential_scope,
#                 emailfrom=emailfrom,
#                 receiver=emailto,
#                 subject=send_email.EMAIL_TEMPLATES["EXISTING_PE_NO_SA_CONN"]["subject"],
#                 message=send_email.EMAIL_TEMPLATES["EXISTING_PE_NO_SA_CONN"]["message"].format(sa_name=storage_account['name'],
#                                                                                             vnet_name=pe_vnet_name,
#                                                                                             subnet_name=pe_subnet_name,
#                                                                                             workspace_url=workspace_url))
#         except Exception as e:
#             logger.error(f"Error sending email: {e}")
#             raise e
#     else:
#         sa_details = azure_read.get_az_storage_account_details(azure_creds, storage_account['name'])

#         if sa_details:
#             logger.info("Fetched Storage Account details successfully. Creating SNOW ticket..")
#             snow = service_now.ServiceNow(instance="instance", username="username", password="password")
#             service_now_order_id = snow.create_order(sa_details)

#             if service_now_order_id:
#                 logger.info(f"SNOW Order {service_now_order_id} created successfully")
#                 try:
#                     workspace_url = "https://" + spark.conf.get("spark.databricks.workspaceUrl")

#                     send_email.sendEmailGeneric(
#                         credential_scope=email_credential_scope,
#                         emailfrom=emailfrom,
#                         receiver=emailto,
#                         subject=send_email.EMAIL_TEMPLATES["NEW_SNOW_ORDER"]["subject"],
#                         message=send_email.EMAIL_TEMPLATES["NEW_SNOW_ORDER"]["message"].format(sa_name=storage_account['name'],
#                                                                                             vnet_name=pe_vnet_name,
#                                                                                             subnet_name=pe_subnet_name,
#                                                                                             snow_order_id=service_now_order_id,
#                                                                                             workspace_url=workspace_url))
#                 except Exception as e:
#                     logger.error(f"Error sending email: {e}")
#                     raise e

#             else:
#                 logger.error("Error creating SNOW ticket")
#                 raise Exception("Error creating SNOW ticket")