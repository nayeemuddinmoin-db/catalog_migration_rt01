from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS_FOR_TABLES, NON_UC_USER_SCHEMAS_FOR_TABLES
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
import re

from typing import Tuple


class Views(Objects):
    """
    This class is responsible for reading and writing UC Views objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class

        Args:
            threads (int): The number of threads to use for parallel processing
        """

        super().__init__(ObjectType.VIEW, "system.information_schema.tables") # Read views from tables table
        
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("view_name"),
            "object_owner": F.col("table_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema")),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
            "object_cross_relationship": F.col("view_name"),
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

        # Views have an additional step to filter out materialized views
        mat_view_df = (
            self._spark.table("system.information_schema.views")
            .filter(F.col("is_materialized") == F.lit(True))
            .select("table_catalog", "table_schema", "table_name")
        )

        df = (
            df.filter(~F.col("table_catalog").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("table_schema").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .filter(F.col("table_type") == F.lit("VIEW"))
            .join( mat_view_df,
                  [df.table_catalog == mat_view_df.table_catalog,
                   df.table_schema == mat_view_df.table_schema,
                   df.table_name == mat_view_df.table_name],
                   "left_anti")
            .withColumn("view_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
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

        if not df.isEmpty():
            view_list = list(map(lambda x: x[0], df.select("view_name").distinct().collect()))
            view_ddl_df = self._parallelize(self.ddl_conversion, view_list, self._threads)

            df = df.join(view_ddl_df, [df.view_name == view_ddl_df.object_name], "left")
        else:
            df = df.withColumn("restore_ddl", F.lit(None).cast(T.StringType()))

        return df

    def ddl_conversion(self, table: str) -> Tuple[str, str]:
        """
        Get the DDL for the views
        
        Args:
            table (str): The table name
            
        Returns:
            Tuple[str, str]: The table name and the DDL
        """
        self._logger.debug(f"Getting the DDL for the view: {table}")

        try:
            view_ddl = self._sql_stmt_with_retry(f"SHOW CREATE TABLE `{table.replace('.', '`.`')}`").collect()[0][0]
            backtick_view = f"`{table.replace('.', '`.`')}`"
            view_ddl = re.sub(r"(?i)^CREATE\s*(?:OR REPLACE)?\s*VIEW(?:\s*IF NOT EXISTS)?\s*[^\(]+", f"CREATE VIEW {backtick_view} ", view_ddl)
        except Exception as e:
            self._logger.warning(f"Error getting the DDL for the views: {e}")
            return (table, None)

        return (table, view_ddl)


class ViewPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Views privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.table_privileges")

        self._update_cols_map({
            "object_name": F.col("view_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("view_name"),
            "object_parent_type": F.lit(ObjectType.VIEW.value),
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

        views_df = (
            self._spark.table("system.information_schema.views")
            .filter(F.col("is_materialized") == F.lit(False)) # Filter out Materialized views
            .withColumn("view_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
            .select("view_name")
        )

        df = (
            df.filter(F.col("inherited_from") == F.lit("NONE"))
            .filter(~F.col("table_catalog").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("table_schema").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .withColumn("view_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
            .join(views_df, "view_name", "inner")
        )

        return df


class ViewTags(Objects):
    """
    This class is responsible for reading and writing UC Views Tags objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """
        super().__init__(ObjectType.TAG, "system.information_schema.table_tags")

        self._update_cols_map({
            "object_name": F.col("view_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.TAG.value),
            "object_parent_name": F.col("view_name"),
            "object_parent_type": F.lit(ObjectType.VIEW.value),
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

        views_df = (
            self._spark.table("system.information_schema.views")
            .filter(F.col("is_materialized") == F.lit(False)) # Filter out Materialized views
            .withColumn("view_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
            .select("view_name")
        )

        df = (
            df.filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("schema_name").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .withColumn("view_name", F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name"), F.lit("."), F.col("table_name")))
            .join(views_df, "view_name", "inner")
        )

        return df