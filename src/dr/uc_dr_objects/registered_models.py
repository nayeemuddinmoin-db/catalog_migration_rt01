from dr.uc_dr_objects.objects import Objects, ObjectType
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from typing import Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import RegisteredModelInfo
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Optional
from itertools import repeat
from functools import reduce
from dataclasses import fields
import json
import datetime as dt
import mlflow


class RegisteredModelObjects(Objects):
    """
    This class is responsible for reading and writing UC Registered Models objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.REGISTERED_MODEL, None)
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("full_name"),
            "object_owner": F.col("owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name")),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
            "object_cross_relationship": F.col("full_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None),
            "api_payload": F.col("api_payload")
        })

    def pre_process(self) -> DataFrame:
        """
        Read registered models in Unity Catalog

        Args:
            dest_storage_path (str): to confirm with base class

        Returns:
            DataFrame: The table read from the start date
        """

        self._logger.info(f"API Based read of UC Registered Models from Unity Catalog")

        w = WorkspaceClient()
        registered_models = w.registered_models.list(max_results=1000) # TODO: Check limits for registered models

        schema = ", ".join([f"{f.name} string" for f in fields(RegisteredModelInfo)] + ["api_payload string"])

        registered_model_df = self._spark.createDataFrame([self._get_registered_model_details(r) for r in registered_models], schema)

        return registered_model_df
    
    def _get_registered_model_details(self, model: RegisteredModelInfo) -> Dict[str, str]:
        """
        Get the registered model details

        Args:
            model (RegisteredModelInfo): The registered model

        Returns:
            Dict[str, str]: The registered model details
        """
        model_dict = model.as_dict()
        details = dict()
        details["registered_model"] = {
            k:model_dict.get(k, None) for k in ["catalog_name", "schema_name", "name", "comment", "storage_location"]
            }
        
        details["versions"] = self._get_registered_model_version_details(model.full_name)

        model_dict["api_payload"] = json.dumps(details)

        return model_dict
    
    def _get_registered_model_version_details(self, reg_model_name: str) -> List[Dict[str, Any]]:
        """
        Get the registered model details

        Args:
            reg_model_name (str): The registered model name

        Returns:
            List[Dict[str, Any]]: The registered model details
        """
        mlflow.set_registry_uri("databricks-uc")

        w = WorkspaceClient()
        c = mlflow.client.MlflowClient()

        model_versions = w.model_versions.list(full_name=reg_model_name, max_results=1000)

        version_details = list()
        for version in model_versions:
            mlflow_model_version_details = c.get_model_version(reg_model_name, version.version)
            reg_model_version_details = w.model_versions.get(full_name=reg_model_name, version=version.version)

            details = {
                "name": reg_model_name,
                "aliases": mlflow_model_version_details.aliases,
                "description": mlflow_model_version_details.description,
                "tags": mlflow_model_version_details.tags,
                "current_stage": mlflow_model_version_details.current_stage,
                "source": reg_model_version_details.storage_location,
                "run_id": mlflow_model_version_details.run_id,
                "version": mlflow_model_version_details.version
            }
            self._logger.debug(details)

            version_details.append(details)

        return version_details

    
class RegisteredModelPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Registered Models Privileges objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, None)
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("registered_model_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("registered_model_name"),
            "object_parent_type": F.lit(ObjectType.REGISTERED_MODEL.value),
            "object_cross_relationship": F.col("registered_model_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None).cast(T.StringType()),
            "api_payload": F.lit(None).cast(T.StringType())
        })

    def read(self, reg_models: List[str], start_ts: dt.datetime) -> DataFrame:
        """
        Read the UC Registered Models Privileges

        Parameters:
            reg_models (List[str]): The list of registered models
            start_ts (datetime): The start timestamp

        Returns:
            DataFrame: The UC Registered Models Privileges
        """

        self._logger.info(f"API Based read of UC Registered Models Privileges from Unity Catalog")

        reg_model_details_df = self._parallelize(self.get_reg_model_details, reg_models, self._threads)

        final_df = (
            reg_model_details_df.transform(self.normalize_privilege_read, ObjectType.FUNCTION.value)
            .transform(self.add_owner_object)
            .transform(self.add_uid)
            .withColumn("created_tsp", F.lit(start_ts))
        )

        return final_df

    def _parallelize(self, function: Callable, object: List[str], threads: int = 8) -> DataFrame:
        """
        Parallelize the function

        Args:
            function (Callable): The function to parallelize
            object (List[str]): The list of objects
            threads (int): The number of threads to use

        Returns:
            List[str]: The list of results
        """

        schema = "registered_model_name string, grantee string, privilege_type string"
        unioned_df = self._spark.createDataFrame([], schema)
        with ThreadPoolExecutor(max_workers=threads) as executor:
            dfs = [ sync_status for sync_status in executor.map(function, object, repeat(schema))]

            unioned_df = reduce(DataFrame.union, dfs, unioned_df)

        return unioned_df
    
    def get_reg_model_details(self, reg_model: str, schema: str) -> DataFrame:
        """
        Get registered model details
        
        Args:
            reg_model (str): The connection name
            schema (str): The schema to use
            
        Returns:
            Tuple[str, str, str]: The registered model details
        """
        self._logger.debug(f"Getting the registered model details for {reg_model}")

        try:
            cols_map = {
                    "ObjectKey": "registered_model_name",
                    "Principal": "grantee",
                    "ActionType": "privilege_type",
            }

            # Currently, the only way to get connection privileges
            reg_models_details_df = (
                self._sql_stmt_with_retry(f"SHOW GRANTS ON FUNCTION `{reg_model.replace('.', '`.`')}`")
                .filter("ObjectType = 'FUNCTION'")
                .withColumnsRenamed(cols_map)
                .select(*cols_map.values())
            )
        except Exception as e:
            self._logger.warning(f"Error getting registered model details: {e}")
            return self._spark.createDataFrame([], schema)

        return reg_models_details_df


class UCRegisteredModels:

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        self.object_type = ObjectType.REGISTERED_MODEL

        self._registered_model_objects = RegisteredModelObjects(threads)
        self._registered_model_privileges = RegisteredModelPrivileges(threads)
        # self._registered_model_tags = RegisteredModelTags(threads)

    def read(self, start_ts: Optional[dt.datetime], **kwargs) -> DataFrame:
        """
        Read the UC Registered Models

        Returns:
            DataFrame: The UC Registered Models
        """
        start_ts = start_ts or dt.datetime.now()

        registered_models_df = self._registered_model_objects.read(start_ts=start_ts)

        registered_models_list = list(map(lambda x: x[0], registered_models_df.select("object_name").distinct().collect()))
        registered_model_privileges_df = self._registered_model_privileges.read(registered_models_list, start_ts)
        # registered_model_tags_df = self._registered_model_tags.read(registered_models_list)

        final_df = (
            registered_models_df.union(registered_model_privileges_df)
            # .union(registered_model_tags_df)
        )

        return final_df