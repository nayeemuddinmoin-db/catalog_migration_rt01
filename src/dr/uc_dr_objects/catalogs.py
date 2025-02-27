from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from typing import Tuple
from databricks.sdk import WorkspaceClient

import json


class Catalogs(Objects):
    """
    This class is responsible for reading and writing UC Catalogs objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.CATALOG, "system.information_schema.catalogs")

        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("catalog_name"),
            "object_owner": F.col("catalog_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.lit(self.metastore_id),
            "object_parent_type": F.lit(ObjectType.METASTORE.value),
            "object_cross_relationship": F.col("catalog_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.col("restore_ddl"),
            "api_payload": F.col("api_payload")
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
            df.filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS)) # reserved catalog names
        )

        return df
    
    def generate_restore_stmt(self, df: DataFrame) -> DataFrame:
        """
        Normalize the read data

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the read data for {self.object_type}")

        catalog_list = list(map(lambda x: x[0], df.select("catalog_name").distinct().collect()))
        catalog_ddl_df = self._parallelize(self.ddl_conversion, catalog_list, self._threads)

        ws_catalog_bindings_df = (
            self._spark.createDataFrame([ self.catalog_binding(catalog) for catalog in catalog_list ], "catalog string, api_payload string")
        )

        df = (
            df.join(catalog_ddl_df, [df.catalog_name == catalog_ddl_df.object_name], "left")
            .join(ws_catalog_bindings_df, [df.catalog_name == ws_catalog_bindings_df.catalog], "left")
        )

        return df
    
    def ddl_conversion(self, catalog: str) -> Tuple[str, str]:
        """
        Get the DDL for the catalogs
        
        Args:
            catalog (str): The catalog name
            
        Returns:
            Tuple[str, str]: The catalog name and the DDL
        """
        self._logger.debug(f"Getting the DDL for the catalog: {catalog}")

        try:
            catalog_details = (
                self._sql_stmt_with_retry(f"DESCRIBE CATALOG EXTENDED `{catalog}`")
                .withColumn("info_name", F.lower(F.replace(F.col("info_name"), F.lit(" "), F.lit("_")).cast(T.StringType())))
                .collect()
            )

            kwargs = {
                row["info_name"] : row["info_value"] for row in catalog_details
            }

            options = kwargs.get("options", False)
            if options:
                try:
                    parsed_options = dict()
                    for option in options.split(","):
                        key, value = option.split("=")
                        parsed_options[key.strip()] = value.strip()

                    kwargs["options"] = parsed_options
                except:
                    # error parsing, leave as is
                    pass

            try:
                if kwargs["catalog_type"].lower() == "delta sharing":
                    w = WorkspaceClient()
                    ds_details = w.catalogs.get(catalog).as_dict()
                    kwargs["provider_name"] = ds_details["provider_name"]
                    kwargs["share_name"] = ds_details["share_name"]
                    kwargs["delta_sharing"] = True
            except Exception as e:
                self._logger.warning(f"Error getting the share for the catalog {catalog}: {e}")

            self._logger.debug(f"Catalog DDL kwargs: {kwargs}")

            catalog_ddl = DDL(ObjectType("CATALOG"))(**kwargs)
            
        except Exception as e:
            self._logger.warning(f"Error getting the DDL for the catalog {catalog}: {e}")
            return (catalog, None)

        return (catalog, catalog_ddl)
    
    def catalog_binding(self, catalog: str) -> Tuple:
        """
        Get the catalog binding for the table
        
        Args:
            catalog (str): The catalog name
            
        Returns:
            Tuple: The catalog binding
        """
        ws_catalog_binding = tuple([catalog, None])

        try:
            client = WorkspaceClient()

            ws_bindings = client.workspace_bindings.get(catalog).workspaces

            self._logger.debug(f"Catalog Workspaces Bindings: {ws_bindings}")

            if ws_bindings not in ([], None):
                ws_catalog_binding = tuple([catalog, json.dumps({"assign_workspaces": ws_bindings})])
        except Exception as e:
            self._logger.warning(f"Error getting the catalog binding for the catalog {catalog}: {e}")

        return ws_catalog_binding
    

class CatalogPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Catalogs privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.catalog_privileges")

        self._update_cols_map({
            "object_name": F.col("catalog_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("catalog_name"),
            "object_parent_type": F.lit(ObjectType.CATALOG.value),
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
            # .filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS))
        )

        return df


class CatalogTags(Objects):
    """
    This class is responsible for reading and writing UC Catalogs Tags objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.TAG, "system.information_schema.catalog_tags")

        self._update_cols_map({
            "object_name": F.col("catalog_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.TAG.value),
            "object_parent_name": F.col("catalog_name"),
            "object_parent_type": F.lit(ObjectType.CATALOG.value),
            "object_cross_relationship": F.lit(None).cast(T.StringType()), # Will be based on tags
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None).cast(T.StringType()),
            "api_payload": F.lit(None).cast(T.StringType()),
        })
