from utilities.restore_restartability_logging import *

from pyspark.sql.types import Row
from databricks.sdk import WorkspaceClient
from typing import List
from pyspark.sql.functions import col
import json
from databricks.sdk.service import catalog
from databricks.sdk.service.catalog import IsolationMode
from utilities.helper_functions import *
import mlflow
import re


class RestoreManager:

    def __init__(self, logger):
        self.logger = logger
        self.uc_object_failed_list = []
        self.wrkspaceclient = WorkspaceClient()
        self.user=spark.sql("select current_user()").head()[0]
        self.permission_setup_pre_step = {"EXTERNAL LOCATION": ["ALL PRIVILEGES"],
                             "CATALOG": ["ALL PRIVILEGES"],
                             "STORAGE CREDENTIAL": ["ALL PRIVILEGES"]
                             }


    def get_uc_objects(self, logger, df_uc_dr_restore:DataFrame, object_type:str) -> List:
        try:
            logger.info("Getting list of all UC objects of type %s" %object_type)
            return df_uc_dr_restore.where(col("object_type")==f'{object_type}').collect()
        except Exception as e:
            logger.error("Failed to get list of uc objects of type %s" %object_type,exc_info=True)

    def create_uc_object(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Creating %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
            spark.sql("%s" %uc_object_info.restore_ddl)
            # if uc_object_info.object_type == 'CATALOG' and uc_object_info.api_payload is not None:
            #    self.wrkspaceclient.catalogs.update(name=uc_object_info.object_name, isolation_mode=IsolationMode.ISOLATED)
            #    self.wrkspaceclient.workspace_bindings.update(name=uc_object_info.object_name, assign_workspaces=json.loads(uc_object_info.api_payload).get("assign_workspaces"))

        except Exception as e:
            try:
                if uc_object_info.object_type == "TABLE": # and ("DELTA_CREATE_TABLE_SCHEME_MISMATCH" in str(e) or "DELTA_CREATE_TABLE_WITH_DIFFERENT_PROPERTY" in str(e)):
                    uc_table_name = uc_object_info.object_name
                    backtick_tbl_name = f"`{uc_table_name.replace('.', '`.`')}`"

                    location_pattern = re.compile("LOCATION\s+'([^']+)'", re.IGNORECASE)
                    location = location_pattern.search(uc_object_info.restore_ddl)

                    # Pattern to extract TBLPROPERTIES section
                    tblproperties_pattern = re.compile(r"TBLPROPERTIES\s*\((.*?)\)", re.DOTALL | re.IGNORECASE)
                    tblproperties_match = tblproperties_pattern.search(uc_object_info.restore_ddl)

                    if location:
                        extracted_location = location.group(1)
                        truncated_ddl = f"""CREATE TABLE {backtick_tbl_name}
                        USING DELTA
                        LOCATION '{extracted_location}'"""
                        spark.sql(truncated_ddl)
                    else:
                        truncated_ddl = f"""CREATE TABLE {backtick_tbl_name}
                        USING DELTA"""
                        spark.sql(truncated_ddl)

                    if tblproperties_match:
                        tblproperties_str = tblproperties_match.group(1)
                        # extracted_property_upgraded_to = f"'upgraded_to' = '{upgraded_to.group(1)}'"
                        try:
                            spark.sql(f"ALTER TABLE {backtick_tbl_name} SET TBLPROPERTIES ({tblproperties_str})")
                        except:
                            pass
                else:
                    failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
                    self.uc_object_failed_list.append(failed_set)
                    # self.uc_object_failed_list.append(uc_object_info.object_uid)
                    if "UnauthorizedAccessException" in str(e):
                        self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, "Unauthorized Access Exception    occurred. PERMISSION_DENIED"))
                    else:
                        self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            except Exception as e:
                failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type,uc_object_info.object_name, str(e)))
                self.uc_object_failed_list.append(failed_set)
                # self.uc_object_failed_list.append(uc_object_info.object_uid)
                if "UnauthorizedAccessException" in str(e):
                    self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name,"Unauthorized Access Exception occurred.      PERMISSION_DENIED"))
                else:
                    self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name,str(e)))


    def grant_permission_to_uc_object(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Granting permission to %s: %s" % (uc_object_info.object_parent_type, uc_object_info.object_name))
            spark.sql("%s" %uc_object_info.restore_ddl)
        
        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to grant permission to %s: %s, because %s" %(uc_object_info.object_parent_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to grant permission to %s: %s" %(uc_object_info.object_parent_type, uc_object_info.object_name), exc_info=True)


    def alter_uc_object(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Running alter %s on: %s" % (uc_object_info.object_type, uc_object_info.object_name))
            spark.sql("%s" %uc_object_info.restore_ddl)
        
        except AnalysisException as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to run alter %s on: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to run alter %s on: %s" %(uc_object_info.object_type, uc_object_info.object_name), exc_info=True)

    def alter_owner_of_uc_object(self, uc_object_info:Row) -> None: 
        try:
            self.logger.info("Running alter owner on %s: %s" % (uc_object_info.object_parent_type, uc_object_info.object_name))
            if uc_object_info.object_parent_type in ('REGISTERED MODEL', 'FUNCTION'):
                api_payload = json.loads(uc_object_info.api_payload)
                self.wrkspaceclient.registered_models.update(full_name=api_payload.get("full_name"), owner=api_payload.get("owner"))     
            else:
                spark.sql("%s" %uc_object_info.restore_ddl)
        
        except AnalysisException as e:
            failed_set = (uc_object_info.object_uid, "Failed to run alter owner on %s: %s, because %s" %(uc_object_info.object_parent_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to run alter owner on %s: %s, because %s" %(uc_object_info.object_parent_type, uc_object_info.object_name, str(e)))
        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to run alter owner on %s: %s, because %s" %(uc_object_info.object_parent_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to run alter owner on %s: %s" %(uc_object_info.object_parent_type, uc_object_info.object_name), exc_info=True)


    def grant_restore_executor_permission_pre_step(self, uc_object_info:Row) -> None:
        self.logger.info("Granting permission to user %s on %s: %s" % (self.user, uc_object_info.object_type, uc_object_info.object_name))
        try:
            spark.sql(f"""GRANT {','.join(self.permission_setup_pre_step.get(uc_object_info.object_type))} ON {uc_object_info.object_type} `{uc_object_info.object_name}` TO `{self.user}`""")
        except Exception as e:
            self.logger.error("Failed to Grant permission to user %s on %s: %s" %(self.user, uc_object_info.object_type, uc_object_info.object_name), exc_info=True)


    def revoke_restore_executor_permission_post_step_cleanup(self, uc_object_info:Row) -> None:
        self.logger.info("Revoking permission from user %s on %s: %s" % (self.user, uc_object_info.object_type, uc_object_info.object_name))
        try:
            spark.sql(f"""REVOKE {','.join(self.permission_setup_pre_step.get(uc_object_info.object_type))} ON {uc_object_info.object_type} `{uc_object_info.object_name}` FROM `{self.user}`""")
        except Exception as e:
            self.logger.error("Failed to Revoke permission from user %s on %s: %s" %(self.user, uc_object_info.object_type, uc_object_info.object_name), exc_info=True)

    def grant_restore_executor_ownership_pre_step(self, uc_object_info:Row) -> None:
        self.logger.info("Granting ownership to user %s on %s: %s" % (self.user, uc_object_info.object_type, uc_object_info.object_name))
        try:
            spark.sql(f"""ALTER {uc_object_info.object_type} {uc_object_info.object_name} SET OWNER TO `{self.user}`""")
        except Exception as e:
            self.logger.error("Failed to Alter ownership to user %s on %s: %s" %(self.user, uc_object_info.object_type, uc_object_info.object_name), exc_info=True)


class RestoreManagerStorageCredential(RestoreManager):
    def __init__(self, logger):
        super().__init__(logger)
        self.wrkspaceclient = WorkspaceClient()

    def get_storage_credential(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of storage credentials to restore")
            return df_uc_dr_restore.where("object_type='STORAGE CREDENTIAL'").collect()

        except Exception as e:
            logger.error("Failed to get list of storage credentials",exc_info=True)

    def create_storage_credential(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Creating %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
            api_payload = json.loads(uc_object_info.api_payload)
            if api_payload.get("azure_managed_identity") is not None:
                created_cred = self.wrkspaceclient.storage_credentials.create(name=api_payload.get("name"), 
                                    azure_managed_identity=(catalog.AzureManagedIdentityRequest(access_connector_id=api_payload.get("azure_managed_identity").get("access_connector_id"),
                                                            managed_identity_id = api_payload.get("azure_managed_identity").get("managed_identity_id")
                                                            )),
                                    read_only = api_payload.get("read_only"),
                                    skip_validation = True)

            elif api_payload.get("azure_service_principal") is not None:
                created_cred = self.wrkspaceclient.storage_credentials.create(name=api_payload.get("name"), 
                                    azure_service_principal=(catalog.AzureServicePrincipal(directory_id=api_payload.get("azure_service_principal").get("directory_id"),
                                                            application_id = api_payload.get("azure_service_principal").get("application_id"),
                                                            client_secret = api_payload.get("azure_service_principal").get("client_secret")
                                                            )),
                                    read_only = api_payload.get("read_only"),
                                    skip_validation = True,
                                    comment = api_payload.get("comment")
                                    )
        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))


class RestoreManagerExternalLocation(RestoreManager):
    def __init__(self, logger):
        super().__init__(logger)

    def create_external_locations(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Creating %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
            spark.sql("%s" %uc_object_info.restore_ddl)
            if uc_object_info.api_payload is not None:
                self.logger.info("Changing to read only %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
                w = WorkspaceClient()
                w.external_locations.update(name='%s' %uc_object_info.object_name,read_only=True)
        except AnalysisException as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to create %s: %s" %(uc_object_info.object_type, uc_object_info.object_name), exc_info=True)

    def get_external_locations(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of external locations to restore")
            return df_uc_dr_restore.where("object_type='EXTERNAL LOCATION'").collect()

        except Exception as e:
            logger.error("Failed to get list of external locations",exc_info=True)

    # def alter_external_location_read_only(self, logger, uc_object_info:Row) -> None:
    #     try:
    #         logger.info("Changing to read only %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
    #         w = WorkspaceClient()
    #         w.external_locations.update(name='%s' %uc_object_info.object_name,read_only=True)
        
    #     except Exception as e:
    #         logger.error("Failed to change to read only %s: %s" %(uc_object_info.object_type, uc_object_info.object_name), exc_info=True)



class RestoreManagerConnection(RestoreManager):
    def __init__(self, logger):
        super().__init__(logger)

    def get_connections(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of connections to restore")
            return df_uc_dr_restore.where("object_type='CONNECTION'").collect()

        except Exception as e:
            logger.error("Failed to get list of connections",exc_info=True)



class RestoreManagerShare(RestoreManager):
    def __init__(self, logger):
        super().__init__(logger)

    def get_shares(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of shares to restore")
            return df_uc_dr_restore.where("object_type='SHARE'").collect()

        except Exception as e:
            logger.error("Failed to get list of shares",exc_info=True)



class RestoreManagerRecipient(RestoreManager):
    def __init__(self, logger):
        super().__init__(logger)

    def create_recipients(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Creating %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
            spark.sql("%s" %uc_object_info.restore_ddl)
            if uc_object_info.api_payload is not None:
                self.logger.info("Updating IP Access List of %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
                w = WorkspaceClient()
                w.recipients.update(ip_access_list=json.loads(uc_object_info.api_payload).get('ip_access_list').get('allowed_ip_addresses'))
        except AnalysisException as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            # self.uc_object_failed_list.append(uc_object_info.object_uid)
            self.logger.error("Failed to create %s: %s" %(uc_object_info.object_type, uc_object_info.object_name), exc_info=True)

    def get_recipients(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of recipients to restore")
            return df_uc_dr_restore.where("object_type='RECIPIENT'").collect()

        except Exception as e:
            logger.error("Failed to get list of recipients",exc_info=True)



class RestoreManagerCatalog(RestoreManager):
    def __init__(self,logger):
        super().__init__(logger)

    def get_catalogs(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of catalogs to restore")
            return df_uc_dr_restore.where("object_type='CATALOG'").collect()

        except Exception as e:
            logger.error("Failed to get list of catalogs",exc_info=True)



class RestoreManagerSchema(RestoreManager):
    def __init__(self,logger):
        super().__init__(logger)

    def get_schemas(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of schemas to restore")
            return df_uc_dr_restore.where("object_type='SCHEMA'").collect()

        except Exception as e:
            logger.error("Failed to get list of schemas",exc_info=True)



class RestoreManagerTable(RestoreManager):
    def __init__(self,logger):
        super().__init__(logger)

    def get_tables(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of tables to restore")
            return df_uc_dr_restore.where("object_type='TABLE'").collect()

        except Exception as e:
            logger.error("Failed to get list of tables",exc_info=True)



class RestoreManagerView(RestoreManager):
    def __init__(self,logger):
        super().__init__(logger)

    def get_views(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of views to restore")
            return df_uc_dr_restore.where("object_type='VIEW'").collect()

        except Exception as e:
            logger.error("Failed to get list of views",exc_info=True)



class RestoreManagerFunction(RestoreManager):
    def __init__(self,logger):
        super().__init__(logger)

    def get_functions(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of functions to restore")
            return df_uc_dr_restore.where("object_type='FUNCTION'").collect()

        except Exception as e:
            logger.error("Failed to get list of functions",exc_info=True)



class RestoreManagerVolume(RestoreManager):
    def __init__(self,logger):
        super().__init__(logger)

    def get_volumes(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of volumes to restore")
            return df_uc_dr_restore.where("object_type='VOLUME'").collect()

        except Exception as e:
            logger.error("Failed to get list of volumes",exc_info=True)



class RestoreManagerRegisteredModel(RestoreManager):
    def __init__(self, logger):
        super().__init__(logger)
        mlflow.set_registry_uri('databricks-uc')
        self.client = mlflow.MlflowClient()

    def get_registered_model(self, logger, df_uc_dr_restore:DataFrame) -> List:
        try:
            logger.info("Getting list of registered model to restore")
            return df_uc_dr_restore.where("object_type='REGISTERED MODEL'").collect()

        except Exception as e:
            logger.error("Failed to get list of registered model",exc_info=True)
        
    def create_registered_model(self, uc_object_info:Row) -> None:
        try:
            self.logger.info("Creating %s: %s" % (uc_object_info.object_type, uc_object_info.object_name))
            api_payload = json.loads(uc_object_info.api_payload)
            full_model_name = '.'.join([api_payload.get("registered_model").get("catalog_name"),api_payload.get("registered_model").get("schema_name"),api_payload.get("registered_model").get("name")])

            model_description = api_payload.get("registered_model").get("comment")
            self.client.create_registered_model(name=full_model_name, description=model_description)

            self.logger.info("Getting model versions of the %s: %s" %(uc_object_info.object_type, uc_object_info.object_name))
            model_versions_list = api_payload.get("versions")
            model_versions_sorted_list = sorted(model_versions_list, key=lambda x: int(x["version"]))

            for ver in model_versions_sorted_list:
                self.logger.info("Creating model version of: %s: %s" %(ver["name"], ver["source"]))
                dbfs_source = f"dbfs:/model_backup/{ver['name'].replace('.','_')}/{ver['version']}/"
                self.logger.info("Copying source artifacts to dbfs path - %s" %dbfs_source)
                dbutils.fs.cp(ver['source'], dbfs_source, recurse=True)
                self.logger.info("Copy of source artifacts completed")
                vn = self.client.create_model_version(name=ver["name"], source=dbfs_source, run_id=ver["run_id"], description=ver["description"])
                for alias in ver.get("aliases"):
                    self.client.set_registered_model_alias(name=ver["name"], alias=alias, version=vn.version)

        except Exception as e:
            failed_set = (uc_object_info.object_uid, "Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))
            self.uc_object_failed_list.append(failed_set)
            self.logger.error("Failed to create %s: %s, because %s" %(uc_object_info.object_type, uc_object_info.object_name, str(e)))