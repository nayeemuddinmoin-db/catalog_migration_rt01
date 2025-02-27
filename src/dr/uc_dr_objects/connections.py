from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List
from itertools import repeat
from functools import reduce

import ast


class Connections(Objects):
    """
    This class is responsible for reading and writing UC Connections objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.CONNECTION, "system.information_schema.connections")

        self._update_cols_map({
            "object_name": F.col("connection_name"),
            "object_owner": F.col("connection_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.lit(self.metastore_id),
            "object_parent_type": F.lit(ObjectType.METASTORE.value),
            "object_cross_relationship": F.col("connection_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.col("restore_ddl"),
            "api_payload": F.lit(None).cast(T.StringType())
        })

    @staticmethod
    @F.udf(returnType=T.StringType())
    def ddl_conversion(cols: Column) -> T.StringType:
        """
        Convert the DataFrame columns to a DDL statement

        Args:
            cols (Column): The DataFrame columns

        Returns:
            T.StringType: The DDL statement
        """
        kwargs = {
            "connection_name": cols.connection_name,
            "connection_type": cols.connection_type,
            "options": ast.literal_eval(cols.connection_options),
            "comment": cols.comment,
        }

        connection_ddl = DDL(ObjectType.CONNECTION)

        return connection_ddl(**kwargs)


class ConnectionPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Connection objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.connections")
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("connection_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("connection_name"),
            "object_parent_type": F.lit(ObjectType.CONNECTION.value),
            "object_cross_relationship": F.lit(None).cast(T.StringType()), # Will be based on privileges
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None).cast(T.StringType()),
            "api_payload": F.lit(None).cast(T.StringType())
        })
    
    def normalize_privilege_read(self, df: DataFrame) -> DataFrame:
        """
        Normalize the read data

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the read data for {self.object_type}")

        connection_list = list(map(lambda x: x[0], df.select("connection_name").distinct().collect()))
        connection_details_df = self._parallelize(self.get_conn_details, connection_list, self._threads)

        df = (
            df.join(connection_details_df, "connection_name", "left")
            .filter("privilege_type is not null")
        )

        return super().normalize_privilege_read(df)
    
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

        schema = "connection_name string, grantee string, privilege_type string"
        unioned_df = self._spark.createDataFrame([], schema)
        with ThreadPoolExecutor(max_workers=threads) as executor:
            dfs = [ sync_status for sync_status in executor.map(function, object, repeat(schema))]

            unioned_df = reduce(DataFrame.union, dfs, unioned_df)

        return unioned_df
    
    def get_conn_details(self, connection: str, schema: str) -> DataFrame:
        """
        Get Connection details
        
        Args:
            connection (str): The connection name
            schema (str): The schema to use
            
        Returns:
            Tuple[str, str, str]: The connection details
        """
        self._logger.debug(f"Getting the connection details for {connection}")

        try:
            cols_map = {
                    "ObjectKey": "connection_name",
                    "Principal": "grantee",
                    "ActionType": "privilege_type",
            }

            # Currently, the only way to get connection privileges
            connection_details_df = (
                self._sql_stmt_with_retry(f"SHOW GRANTS ON CONNECTION `{connection}`")
                .filter("ObjectType = 'CONNECTION'")
                .withColumnsRenamed(cols_map)
                .select(*cols_map.values())
            )
        except Exception as e:
            self._logger.warning(f"Error getting connection details: {e}")
            return self._spark.createDataFrame([], schema)

        return connection_details_df
