from dr.uc_dr_objects.objects import Objects, ObjectType
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from typing import Tuple
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import AzureManagedIdentityResponse, AzureServicePrincipal
import json


class StorageCredentials(Objects):
    """
    This class is responsible for reading and writing UC Storage Credentials objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.STORAGE_CREDENTIAL, "system.information_schema.storage_credentials")
        self._update_cols_map({
            "object_name": F.col("storage_credential_name"),
            "object_owner": F.col("storage_credential_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.lit(self.metastore_id),
            "object_parent_type": F.lit(ObjectType.METASTORE.value),
            "object_cross_relationship": F.col("storage_credential_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None),
            "api_payload": F.col("api_payload")
        })

    def generate_restore_stmt(self, df: DataFrame) -> DataFrame:
        """
        Read Storage Credentials in Unity Catalog

        Returns:
            DataFrame: The table read from the start date
        """

        self._logger.info(f"API Based read of UC Storage Credential from Unity Catalog")

        storage_credentials = list(map(lambda x: x[0], df.select("storage_credential_name").distinct().collect()))

        storage_credentials_df = (
            self._spark.createDataFrame([ self.get_storage_credentials_details(st) for st in storage_credentials ], 
                                        "storage_credential_name string, api_payload string")
        )

        df = df.join(storage_credentials_df, "storage_credential_name", "left")

        return df
    
    def get_storage_credentials_details(self, storage_credential: str) -> Tuple:
        """
        Get Storage Credentials
        
        Args:
            catalog (str): The catalog name
            
        Returns:
            Tuple: The catalog binding
        """
        client = WorkspaceClient()

        storage_credential_details = client.storage_credentials.get(storage_credential)

        api_payload = {
            "name": storage_credential_details.name,
            "comment": storage_credential_details.comment,
            "ready_only": storage_credential_details.read_only
        }

        if isinstance(storage_credential_details.azure_managed_identity, AzureManagedIdentityResponse):
            api_payload["azure_managed_identity"] = {
                "access_connector_id": storage_credential_details.azure_managed_identity.access_connector_id,
                "managed_identity_id": storage_credential_details.azure_managed_identity.managed_identity_id,
                "credential_id": storage_credential_details.azure_managed_identity.credential_id
            }
        elif isinstance(storage_credential_details.azure_managed_identity, AzureServicePrincipal):
            api_payload["azure_managed_identity"] = {
                "directory_id": storage_credential_details.azure_service_principal.directory_id,
                "application_id": storage_credential_details.azure_service_principal.application_id,
                "client_secret": storage_credential_details.azure_service_principal.client_secret
            }
        else:
            self._logger.error(f"Invalid Azure Managed Identity Type: {storage_credential_details.azure_managed_identity}")
            api_payload["azure_managed_identity"] = None

        return tuple([storage_credential, json.dumps(api_payload)])
    

class StorageCredentialPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Storage Credential objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.storage_credential_privileges")

        self._update_cols_map({
            "object_name": F.col("storage_credential_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("storage_credential_name"),
            "object_parent_type": F.lit(ObjectType.STORAGE_CREDENTIAL.value),
            "object_cross_relationship": F.lit(None).cast(T.StringType()), # Will be based on privileges
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None).cast(T.StringType()),
            "api_payload": F.lit(None).cast(T.StringType())
        })

    def pre_process(self, df: DataFrame) -> DataFrame:
        """
        Filter the read data
        
        Args:    
            df (DataFrame): The DataFrame to filter
            
        Returns:
            DataFrame: The filtered DataFrame
        """
        self._logger.info(f"Filtering the read data for {self.object_type}")

        df = (
            df.filter(F.col("inherited_from") == F.lit("NONE"))
        )

        return df