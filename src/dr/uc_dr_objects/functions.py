from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS_FOR_TABLES, NON_UC_USER_SCHEMAS_FOR_TABLES
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
import re

from typing import Tuple, Dict, List


class Functions(Objects):
    """
    This class is responsible for reading and writing UC Routines (functions) objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """
        
        super().__init__(ObjectType.FUNCTION, "system.information_schema.routines")
        
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("function_name"),
            "object_owner": F.col("routine_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.concat(F.col("specific_catalog"), F.lit("."), F.col("specific_schema")),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
            "object_cross_relationship": F.col("function_name"),
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
            df.filter(~F.col("specific_catalog").isin(NON_UC_USER_CATALOGS_FOR_TABLES)) # reserved catalog names
            .filter(~F.col("specific_schema").isin(NON_UC_USER_SCHEMAS_FOR_TABLES)) # reserved schema names
            .withColumn("function_name", F.concat(F.col("specific_catalog"), F.lit("."), F.col("specific_schema"), F.lit("."), F.col("specific_name")))
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

        function_list = list(map(lambda x: x[0], df.select("function_name").distinct().collect()))
        function_ddl_df = self._parallelize(self.ddl_conversion, function_list, self._threads)
        
        df = df.join(function_ddl_df, [df.function_name == function_ddl_df.object_name], "left")

        return df

    def ddl_conversion(self, function: str) -> Tuple[str, str]:
        """
        Get the DDL for the routines
        
        Args:
            function (str): The routine name
            
        Returns:
            Tuple[str, str]: The routine name and the DDL
        """
        self._logger.debug(f"Getting the DDL for the routine: {function}")

        try:
            function_details = (
                self._sql_stmt_with_retry(f"DESCRIBE FUNCTION EXTENDED `{function.replace('.', '`.`')}`")
                .collect()
            )

            kwargs = self._parse_function_args(function_details)

            self._logger.debug(f"Function DDL kwargs: {kwargs}")

            function_ddl = DDL(self.object_type)(**kwargs)
            
        except Exception as e:
            self._logger.warning(f"Error getting the DDL for the catalog {function}: {e}")
            return (function, None)

        return (function, function_ddl)
    
    def _parse_function_args(self, function_details: List[Row]) -> Dict[str, str]:
        """
        Parse the routine arguments
        
        Args:
            routine_details (List[Row]): The routine details
            
        Returns:
            Dict[str, str]: The routine arguments
        """
        self._logger.debug(f"Parsing the routine arguments: {function_details}")

        input_rows = False
        return_rows = False
        kwargs = {"type": "scalar", "lang": "sql"} # default settings

        for row in function_details:
            if re.match("function:", row.function_desc, re.IGNORECASE):
                kwargs["function_name"] = f"`{row.function_desc[9:].strip().replace('.', '`.`')}`"
            elif re.match("type:", row.function_desc, re.IGNORECASE):
                kwargs["type"] = row.function_desc[5:].strip().lower()
            elif re.match("input:", row.function_desc, re.IGNORECASE):
                input_rows = True
                kwargs["inputs"] = [row.function_desc[6:].strip()]
            elif re.match("returns:", row.function_desc, re.IGNORECASE):
                input_rows = False
                return_rows = True
                kwargs["return"] = [row.function_desc[8:].strip()]
            elif re.match("comment:", row.function_desc, re.IGNORECASE):
                kwargs["comment"] = row.function_desc[8:].strip()
            elif re.match("body:", row.function_desc, re.IGNORECASE):
                kwargs["body"] = row.function_desc[5:].strip()
            elif re.match("deterministic:", row.function_desc, re.IGNORECASE):
                return_rows = False
            elif re.match("language:", row.function_desc, re.IGNORECASE):
                kwargs["language"] = row.function_desc[9:].strip().lower()
            elif input_rows:
                kwargs["inputs"].append(row.function_desc.strip())
            elif return_rows:
                kwargs["return"].append(row.function_desc.strip())

        return kwargs
    

class FunctionsPrivileges(Objects):
    """
    This class is responsible for reading and writing UC Schema privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.routine_privileges")

        self._update_cols_map({
            "object_name": F.concat(F.col("specific_catalog"), F.lit("."), F.col("specific_schema"), F.lit("."), F.col("specific_name")),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.concat(F.col("specific_catalog"), F.lit("."), F.col("specific_schema"), F.lit("."), F.col("specific_name")),
            "object_parent_type": F.lit(ObjectType.FUNCTION.value),
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
            .filter(~F.col("specific_catalog").isin(NON_UC_USER_CATALOGS_FOR_TABLES))
            .filter(~F.col("specific_schema").isin(NON_UC_USER_SCHEMAS_FOR_TABLES))
        )

        return df