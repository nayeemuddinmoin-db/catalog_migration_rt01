from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS_FOR_TABLES, NON_UC_USER_SCHEMAS_FOR_TABLES, BACKUP_TABLE_SCHEMA
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from typing import Dict, Any
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, TableConstraint
from dr.uc_dr_objects.ddl import DDL
from dr.uc_dr_objects.objects import ObjectType
from typing import List, Callable, Optional
from concurrent.futures import ThreadPoolExecutor

import json

BLACKLISTED_DELTA_TABLE_PROPERTIES = [
    "delta.lastCommitTimestamp",
    "delta.lastUpdateVersion",
    "delta.rowTracking.materializedRowCommitVersionColumnName",
    "delta.rowTracking.materializedRowIdColumnName",
    "delta.feature.liquid",
    "delta.columnMapping.maxColumnId",
    "delta.liquid.clusteringColumns"
]

WHITELISTED_DELTA_TABLE_PROPERTIES = [
    # Based on https://learn.microsoft.com/en-us/azure/databricks/delta/table-properties
    # "delta.feature.v2Checkpoint",
    # "delta.minWriterVersion",
    # "delta.feature.rowTracking",
    # "delta.feature.generatedColumns",
    "delta.enableDeletionVectors",
    # "delta.minReaderVersion",
    # "delta.feature.domainMetadata",
    # "delta.feature.identityColumns",
    "delta.feature.allowColumnDefaults",
    "delta.enableRowTracking",
    "delta.checkpointPolicy",
    # "delta.feature.deletionVectors",
    "delta.appendOnly",
    "delta.autoOptimize.autoCompact",
    "delta.autoOptimize.optimizeWrite",
    "delta.checkpoint.writeStatsAsJson",
    "delta.checkpoint.writeStatsAsStruct",
    "delta.columnMapping.mode",
    "delta.dataSkippingNumIndexedCols",
    "delta.dataSkippingStatsColumns",
    "delta.deletedFileRetentionDuration",
    "delta.isolationLevel",
    "delta.logRetentionDuration",
    "delta.randomizeFilePrefixes",
    "delta.randomPrefixLength",
    "delta.setTransactionRetentionDuration",
    "delta.targetFileSize",
    "delta.tuneFileSizesForRewrites"
]


class Tables(Objects):
    """
    This class is responsible for reading and writing UC Tables objects for Disaster Recovery
    """

    def __init__(self, threads: int = 4) -> None:
        """
        Initializes the class

        Args:
            threads (int): The number of threads to use for parallel processing
        """

        super().__init__(ObjectType.TABLE, "system.information_schema.tables")
        
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("table_name"),
            "object_owner": F.col("table_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.col("object_parent_name"), #F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema")),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
            "object_cross_relationship": F.col("table_name"),
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
            df.filter(~F.col("table_catalog").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("table_schema").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .filter(F.col("table_type").isin(["EXTERNAL", "MANAGED"]))
            # .filter(F.col("is_insertable_into") == F.lit("YES"))
            .withColumn("table_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
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

        self._logger.debug(f"Tables updated since last execution: {df.count()}")

        all_tables_df = (
            self._spark.table(self._source_table)
            .transform(self.pre_process)
            .select("table_name")
            .distinct()
        )
        try:
            backed_up_tables_df = (
                self._spark.read.format("delta").load(self._dest_storage_path)
                .filter("object_type = 'TABLE' and restore_ddl is not null") # guarantee restore_ddl erros will not be kept without retry
                .selectExpr("object_name as table_name")
                .distinct()
            )
            
            new_tables_behind_df = (
                all_tables_df.subtract(backed_up_tables_df)
                .distinct()
            )
        except:
            new_tables_behind_df = all_tables_df

        tables_to_be_updated = df.select("table_name").union(new_tables_behind_df).distinct().collect() 

        table_list = list(map(lambda x: x[0], tables_to_be_updated))
        self._logger.debug(f"Tables that will be processed in this run: {len(table_list)}")
        table_ddl_df = self._parallelize_sdk(self.ddl_conversion, table_list, self._threads)

        return table_ddl_df
    
    def _parallelize_sdk(self, function: Callable, objects: List[str], threads: int) -> DataFrame:
        """
        Parallelize the backup process

        Args:
            function (Callable): The function to parallelize
            objects (List[Any]): The list of objects to parallelize
            threads (int): The number of threads to use
            chunk_size (int): The chunk size

        Returns:
            DataFrame: The backup DataFrame
        """
        schema = T.StructType([
            T.StructField("table_name", T.StringType(), True),
            T.StructField("table_owner", T.StringType(), True),
            T.StructField("object_parent_name", T.StringType(), True),
            T.StructField("object_parent_type", T.StringType(), True),
            T.StructField("object_uid", T.LongType(), True),
            T.StructField("restore_ddl", T.StringType(), True),
            T.StructField("api_payload", T.StringType(), True)
        ])

        with ThreadPoolExecutor(max_workers=threads) as executor:
            results = [result for result in executor.map(function, objects)]
        
        return self._spark.createDataFrame(results, schema)

    def ddl_conversion(self, table: str) -> Dict[str, Any]:
        """
        Get the DDL for the tables
        
        Args:
            table (str): The table name
            
        Returns:
            Tuple[str, str]: The table name and the DDL
        """
        self._logger.debug(f"Getting the DDL for the table {table}")

        table_info = {}

        table_info["table_name"] = table
        table_info["table_owner"] = None
        table_info["object_type"] = ObjectType.TABLE.value
        table_info["object_parent_name"] = None
        table_info["object_parent_type"] = ObjectType.SCHEMA.value
        table_info["restore_ddl"] = None
        table_info["api_payload"] = None

        try:
            w = WorkspaceClient()
            table_api_info = w.tables.get(full_name=table).as_dict()

            row_filter = self._get_row_filters(table_api_info.get("row_filter"))
            columns_ddl, partitions_ddl = self._get_columns_ddl(table_api_info.get("columns"))
            clusters_ddl = self._get_clusters_ddl(table_api_info.get("properties"))
            properties = self._get_table_properties(table_api_info.get("properties"))
            constraints = self._get_table_constraints(table_api_info.get("table_constraints"))

            if constraints and columns_ddl:
                columns_ddl += f",\n{constraints}"

            ddl_kwargs = {
                "full_name": f"`{table_api_info.get('full_name').replace('.', '`.`')}`",
                "table_type": table_api_info.get("table_type"),
                "columns_ddl": columns_ddl,
                "partition_columns": partitions_ddl,
                "cluster_columns": clusters_ddl,
                "comment": table_api_info.get("comment"),
                "data_source_format": table_api_info.get("data_source_format"),
                "storage_location": table_api_info.get("storage_location"),
                "storage_credential": table_api_info.get("storage_credential_name"),
                "row_filter": row_filter,
                "properties": properties
            }

            table_info["table_owner"] = table_api_info.get("owner")
            table_info["object_parent_name"] = f"{table_api_info.get('catalog_name')}.{table_api_info.get('schema_name')}"
            table_info["restore_ddl"] = DDL(ObjectType.TABLE)(**ddl_kwargs)

        except Exception as e:
            self._logger.warning(f"Error getting the DDL for the tables: {e}")

        return table_info

    def _get_columns_ddl(self, columns: ColumnInfo) -> List[str]:
        """
        Get the DDL for the columns
        
        Args:
            columns (list): The columns
            
        Returns:
            List[str]: The columns DDL, partitions DDL
        """

        if columns in (None, []):
            return "", ""

        columns_ddl = []
        partitions = []
        partitions_ddl = None

        for col in sorted(columns, key=lambda x: x["position"]):

            if col.get("partition_index") != None:
                partitions.append((col.get("partition_index"), col.get("name")))

            try:
                col_metadata = json.loads(col.get("type_json")).get("metadata")
            except:
                col_metadata = {}

            col_ddl = f"`{col.get('name')}` {col.get('type_text')}"

            if col_metadata.get("delta.generationExpression") not in (None, {}):
                # GENERATED ALWAYS AS (col_metadata.get("delta.generationExpression"))
                col_ddl += f" GENERATED ALWAYS AS ({col_metadata.get('delta.generationExpression')})"
            elif col_metadata.get("delta.identity.start") not in (None, {}):
                # GENERATED { ALWAYS | BY DEFAULT } AS IDENTITY [ ( [ START WITH start ] [ INCREMENT BY step ] ) ]
                generated_col_ddl = " GENERATED"
                if col_metadata.get("delta.identity.allowExplicitInsert"):
                    generated_col_ddl += " BY DEFAULT"
                else:
                    generated_col_ddl += " ALWAYS"
                generated_col_ddl += f" AS IDENTITY (START WITH {col_metadata.get('delta.identity.start')} INCREMENT BY {col_metadata.get('delta.identity.step')})"

                col_ddl += generated_col_ddl
            elif col_metadata.get("default") not in (None, {}):
                # DEFAULT default_expr
                col_ddl += f" DEFAULT {col_metadata.get('default')}"
            elif col.get("mask"):
                # MASK function_name USING COLUMNS (column_name [, ...])
                col_mask = col.get("mask")
                function_name = f"`{col_mask.get('function_name').replace('.', '`.`')}`"

                col_ddl += f" MASK {function_name}"
                if col_mask.get('using_column_names') not in (None, []):
                    using_column_names = [f"`{col}`" for col in col_mask.get('using_column_names')]
                    col_ddl += f" USING COLUMNS ({', '.join(using_column_names)})"
            elif not col.get("nullable"):
                col_ddl += " NOT NULL"

            if col.get("comment"):
                col_ddl += f" COMMENT '{col.get('comment')}'"
            columns_ddl.append(col_ddl)

        if partitions:
            partitions_ddl = ", ".join([f"`{col[1]}`" for col in sorted(partitions, key=lambda x: x[0])])

        return ",\n".join(columns_ddl), partitions_ddl
    

    def _get_clusters_ddl(self, properties: Dict[str, Any]) -> Optional[str]:
        """
        Get the DDL for the clusters
        
        Args:
            properties (dict): The properties
            
        Returns:
            Optional[str]: The clusters DDL
        """
        if properties in (None, {}):
            return None
        
        cluster_cols = properties.get("delta.liquid.clusteringColumns")

        if cluster_cols in (None, []):
            return None
        
        return ", ".join([f"`{col}`" for col in cluster_cols])
    
    def _get_row_filters(self, row_filter: Dict[str, Any]) -> Optional[str]:
        """
        Get the DDL for the row filters
        
        Args:
            row_filter (dict): The row filter details
            
        Returns:
            Optional[str]: The row filters DDL
        """
        if row_filter in (None, {}):
            return None
        
        self._logger.debug(f"Row filters: {row_filter}")

        try:
            input_column_names = row_filter.get('input_column_names')
            function_name = f"`{row_filter.get('name').replace('.', '`.`')}`"

            # ROW FILTER func_name ON ( [ column_name [, ...] ] ) } [...]
            row_filter_ddl = f"ROW FILTER {function_name}"
            if input_column_names not in (None, []):
                input_column_names = [f"`{col}`" for col in input_column_names]
                row_filter_ddl += f" ON ({', '.join(input_column_names)})"
        except Exception as e:
            self._logger.warning(f"Error getting the row filters DDL: {e}")
            row_filter_ddl = None
        
        return row_filter_ddl
    
    def _get_table_properties(self, properties: Dict[str, Any]) -> Dict[str, Any]:
        """
        Get the DDL for the table properties
        
        Args:
            properties (dict): The properties
            
        Returns:
            Dict[str, Any]: The table properties DDL
        """
        if properties in (None, {}):
            return {}
        
        table_properties = {}

        for key, value in properties.items():
            if not key.startswith("delta."):
                table_properties[key] = value
            elif key in WHITELISTED_DELTA_TABLE_PROPERTIES and key not in BLACKLISTED_DELTA_TABLE_PROPERTIES:
                table_properties[key] = value

        return table_properties

    def _get_table_constraints(self, constraints: List[TableConstraint]) -> Optional[str]:
        """
        Get the DDL for the table constraints
        
        Args:
            constraints (list): The constraints
            
        Returns:
            Optional[str]: The table constraints DDL
        """
        if constraints in (None, []):
            return None
        
        constraints_ddl = []

        for constraint in constraints:
            pk_constraint = constraint.get("primary_key_constraint")
            fk_constraints = constraint.get("foreign_key_constraint")

            if pk_constraint not in ([], {}, "", None):
                # PRIMARY KEY (column_name [, ...])
                columns = [f"`{col}`" for col in pk_constraint.get("child_columns")]
                constraints_ddl.append(f"CONSTRAINT {pk_constraint.get('name')} PRIMARY KEY ({', '.join(columns)})")

            if fk_constraints not in ([], {}, "", None):
                # FOREIGN KEY (column_name [, ...])
                child_columns = [f"`{col}`" for col in fk_constraints.get("child_columns")]
                parent_columns = [f"`{col}`" for col in fk_constraints.get("parent_columns")]
                parent_table = fk_constraints.get("parent_table")

                constraints_ddl.append(f"CONSTRAINT {fk_constraints.get('name')} FOREIGN KEY ({', '.join(child_columns)}) REFERENCES {parent_table} ({', '.join(parent_columns)})")

        return ",\n".join(constraints_ddl)


class TablePrivileges(Objects):
    """
    This class is responsible for reading and writing UC Tables privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.table_privileges")

        self._update_cols_map({
            "object_name": F.col("table_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("table_name"),
            "object_parent_type": F.lit(ObjectType.TABLE.value),
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

        tables_df = (
            self._spark.table("system.information_schema.tables")
            .filter(F.col("table_type").isin(["EXTERNAL", "MANAGED"]))
            .withColumn("table_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
            .select("table_name")
        )

        df = (
            df.filter(F.col("inherited_from") == F.lit("NONE"))
            .filter(~F.col("table_catalog").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("table_schema").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .withColumn("table_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
            .join(tables_df, "table_name", "inner")
        )

        return df


class TableTags(Objects):
    """
    This class is responsible for reading and writing UC Table Tags objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """
        super().__init__(ObjectType.TAG, "system.information_schema.table_tags")

        self._update_cols_map({
            "object_name": F.col("table_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.TAG.value),
            "object_parent_name": F.col("table_name"),
            "object_parent_type": F.lit(ObjectType.TABLE.value),
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

        tables_df = (
            self._spark.table("system.information_schema.tables")
            .filter(F.col("table_type").isin(["", "MANAGED"]))
            .withColumn("table_name", F.concat(F.col("table_catalog"), F.lit("."), F.col("table_schema"), F.lit("."), F.col("table_name")))
            .select("table_name")
        )

        df = (
            df.filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("schema_name").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .withColumn("table_name", F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name"), F.lit("."), F.col("table_name")))
            .join(tables_df, "table_name", "inner")
        )

        return df
