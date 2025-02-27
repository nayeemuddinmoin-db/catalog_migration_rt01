from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS, NON_UC_USER_SCHEMAS
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Column
from typing import Tuple

from databricks.sdk import WorkspaceClient


class Schemas(Objects):
    """
    This class is responsible for reading and writing UC Schemas objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.SCHEMA, "system.information_schema.schemata")

        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("schema_name"),
            "object_owner": F.col("schema_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.col("catalog_name"),
            "object_parent_type": F.lit(ObjectType.CATALOG.value),
            "object_cross_relationship": F.col("schema_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.col("restore_ddl"),
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
            df.filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS)) # reserved catalog names
            .filter(~F.col("schema_name").isin(NON_UC_USER_SCHEMAS)) # reserved schema names
            .withColumn("schema_name", F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name")))
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

        schema_list = list(map(lambda x: x[0], df.select("schema_name").distinct().collect()))
        schema_ddl_df = self._parallelize(self.ddl_conversion, schema_list, self._threads)

        df = (
            df.join(schema_ddl_df, [df.schema_name == schema_ddl_df.object_name], "left")
        )

        return df
    
    def ddl_conversion(self, schema: str) -> Tuple[str, str]:
        """
        Get the DDL for the catalogs
        
        Args:
            schema (str): The schema name
            
        Returns:
            Tuple[str, str]: The schema name and the DDL
        """
        self._logger.debug(f"Getting the DDL for the schema: {schema}")

        try:
            w = WorkspaceClient()

            schema_properties = w.schemas.get(schema).as_dict()
            properties = schema_properties.get("properties", None)
            if properties:
                try:
                    properties.pop("owner")
                except:
                    pass

            kwargs = {
                "schema_name": f"`{schema.replace('.', '`.`')}`",
                "managed_location": schema_properties.get("storage_root", False),
                "comment": schema_properties.get("comment", None),
                "properties": properties if properties not in ({}, None) else False,
            }

            self._logger.debug(f"Schema DDL kwargs: {kwargs}")

            schema_ddl = DDL(ObjectType("SCHEMA"))(**kwargs)
            
        except Exception as e:
            self._logger.warning(f"Error getting the DDL for the schema {schema}: {e}")
            return (schema, None)

        return (schema, schema_ddl)
    

class SchemaPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Schema privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """
        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.schema_privileges")

        self._update_cols_map({
            "object_name": F.col("schema_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("schema_name"),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
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
            # .filter(~F.col("schema_name").isin(NON_UC_USER_SCHEMAS))
            .withColumn("schema_name", F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name")))
        )

        return df


class SchemaTags(Objects):
    """
    This class is responsible for reading and writing UC Schema Tags objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.TAG, "system.information_schema.schema_tags")

        self._update_cols_map({
            "object_name": F.col("schema_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.TAG.value),
            "object_parent_name": F.col("schema_name"),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
            "object_cross_relationship": F.lit(None).cast(T.StringType()), # Will be based on tags
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None).cast(T.StringType()),
            "api_payload": F.lit(None).cast(T.StringType()),
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
            # df.filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS))
            # .filter(~F.col("schema_name").isin(NON_UC_USER_SCHEMAS))
            df.withColumn("schema_name", F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name")))
        )

        return df
