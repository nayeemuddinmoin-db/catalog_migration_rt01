from abc import abstractmethod, abstractstaticmethod
import datetime as dt
from pyspark.sql import DataFrame
from pyspark.sql import Column
from enum import Enum

import logging
import time

from concurrent.futures import ThreadPoolExecutor
from typing import Callable, List, Dict, Tuple, Optional

import pyspark.sql.types as T
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark import errors as ps_errors


NON_UC_USER_CATALOGS = ["system", "main", "hive_metastore", "spark_catalog", "samples", "__databricks_internal"]
NON_UC_USER_SCHEMAS = ["information_schema", "default"]

NON_UC_USER_CATALOGS_FOR_TABLES = ["system", "hive_metastore", "spark_catalog", "samples", "__databricks_internal"]
NON_UC_USER_SCHEMAS_FOR_TABLES = ["information_schema"]

BACKUP_TABLE_SCHEMA = T.StructType([
        T.StructField("object_name", T.StringType(), False),
        T.StructField("object_type", T.StringType(), False),
        T.StructField("object_parent_name", T.StringType(), False),
        T.StructField("object_parent_type", T.StringType(), False),
        T.StructField("object_cross_relationship", T.StringType(), False),
        T.StructField("object_uid", T.LongType(), False),
        T.StructField("restore_ddl", T.StringType(), False),
        T.StructField("api_payload", T.StringType(), True),
        T.StructField("created_tsp", T.TimestampType(), False),
])


class ObjectType(Enum):
    METASTORE = "METASTORE"
    CATALOG = "CATALOG"
    SCHEMA = "SCHEMA"
    VOLUME = "VOLUME"
    TABLE = "TABLE"
    VIEW = "VIEW"
    FUNCTION = "FUNCTION"
    REGISTERED_MODEL = "REGISTERED MODEL"
    STORAGE_CREDENTIAL = "STORAGE CREDENTIAL"
    EXTERNAL_LOCATION = "EXTERNAL LOCATION"
    CONNECTION = "CONNECTION"
    DELTA_SHARING_RECIPIENT = "RECIPIENT"
    DELTA_SHARING_SHARE = "SHARE"
    DELTA_SHARING_SHARE_OBJECT = "DELTA SHARING SHARE OBJECT"
    PRIVILEGE = "PRIVILEGE"
    TAG = "TAG"
    OWNER = "OWNER"


class BackupMode(Enum):
    FRESH_BACKUP = "FRESH_BACKUP"
    INCREMENTAL_BACKUP = "INCREMENTAL_BACKUP"


class Objects:

    def __init__(self, object_type: ObjectType, source_table: Optional[str] = None, **kwargs: Dict) -> None:
        """
        Initialize the object

        Args:
            object_type (ObjectType): The object type
            source_table (str, optional): The source table. Defaults to None.
            kwargs (Dict): The keyword arguments
        """
        
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(level=logging.DEBUG)  # Prevents changing other log levels

        self._object_type = object_type
        self._source_table = source_table
        self._spark = SparkSession.builder.getOrCreate()
        try:
            self._metastore_id = self._sql_stmt_with_retry("SELECT current_metastore()").collect()[0][0]
        except:
            self._metastore_id = None
        self._dest_storage_path = None
        self._backup_mode = BackupMode.INCREMENTAL_BACKUP
        self._kwargs = kwargs
        self._created_tsp = None

        self._schema = BACKUP_TABLE_SCHEMA

    @property
    def object_type(self) -> ObjectType:
        """
        Get the object type
        
        Returns:
            ObjectType: The object type
        """
        return self._object_type

    @property
    def source_table(self) -> str:
        """
        Get the source table

        Returns:
            str: The source table
        """
        return self._source_table
    
    @property
    def backup_mode(self) -> BackupMode:
        """
        Get the backup mode

        Returns:
            BackupMode: The backup mode
        """
        return self._backup_mode
    
    @property
    def metastore_id(self) -> str:
        """
        Get the metastore id

        Returns:
            str: The metastore id
        """
        return self._metastore_id

    # @abstractmethod
    def read(self, dest_storage_path: Optional[str] = None, start_ts: Optional[dt.datetime] = None, backup_mode: BackupMode = BackupMode.INCREMENTAL_BACKUP) -> DataFrame:
        """
        Read table incrementally from the start date

        Args:
            dest_storage_path (str): The destination storage path to store the backup
            start_ts (datetime): The start date to read from

        Returns:
            DataFrame: The table read from the start date
        """

        self._dest_storage_path = dest_storage_path or self._dest_storage_path
        self._backup_mode = backup_mode or self.backup_mode

        if self.object_type != ObjectType.REGISTERED_MODEL:
            self._logger.info(f"Reading new data from {self._source_table}")

            try:
                base_df = self._spark.table(self._source_table)
            except Exception as e:
                self._logger.error(f"Error reading source table {self._source_table}: {str(e)}")
                return self._spark.createDataFrame([], BACKUP_TABLE_SCHEMA)
        else:
            self._logger.info(f"Reading new data from API")

        self._created_tsp = start_ts or dt.datetime.now()
        latest_update = None

        # Privilege and Tag updates are not propagated to the objetcs in system tables, so we can't filter by last_altered
        match self.object_type:
            case ObjectType.TAG:
                norm_df = (
                    base_df.transform(self.pre_process) # filter out non-uc user objects
                    .transform(self.normalize_tag_read)
                )
            case ObjectType.PRIVILEGE:
                norm_df = (
                    base_df.transform(self.pre_process) # filter out non-uc user objects
                    .transform(self.normalize_privilege_read)
                )
            case ObjectType.REGISTERED_MODEL:
                norm_df = (
                    self.pre_process()
                    .transform(self.normalize_object_read)
                )
            case _:
                
                latest_update = self.get_latest_update_tsp()

                norm_df = (
                    base_df.filter(F.col("last_altered") > F.lit(latest_update))
                    .transform(self.pre_process) # filter out non-uc user objects
                    .transform(self.generate_restore_stmt)
                    .transform(self.normalize_object_read)
                )

        final_df = (
            norm_df.transform(self.add_owner_object)
            .transform(self.add_uid)
            .withColumn("created_tsp", F.lit(self._created_tsp))
            .transform(self.read_past_backup_data, latest_update)
        )

        return final_df

    def pre_process(self, df: DataFrame) -> DataFrame:
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

        return df.withColumn("restore_ddl", self.ddl_conversion(F.struct(df.columns)))

    @abstractstaticmethod
    def ddl_conversion(cols: Column) -> T.StringType:
        pass

    def get_latest_update_tsp(self) -> dt.datetime:
        """
        Get the latest update timestamp from the DataFrame

        Returns:
            datetime: The latest update timestamp
        """
        self._logger.info("Getting the latest update timestamp")
        
        if self.object_type != ObjectType.TABLE or self.backup_mode == BackupMode.FRESH_BACKUP:
            self._logger.info("Starting fresh backup from date 1900-01-01.")
            return dt.datetime(1900, 1, 1)

        try:
            self._logger.info("Starting incremental backup")  
            latest_update = (
                self._spark.read.load(self._dest_storage_path)
                .filter(F.col("object_type") == F.lit(self._object_type.value))
                .agg(F.max(F.col("created_tsp")).alias("created_tsp"))
                .collect()[0]["created_tsp"]
            )
        except:
            self._logger.warning(f"Unable to read backup data in path '{self._dest_storage_path}' or it is empty. Setting start date to 1900-01-01.")
            latest_update = dt.datetime(1900, 1, 1)

        return latest_update
    
    def normalize_object_read(self, df: DataFrame) -> DataFrame:
        """
        Normalize the read data for objects

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the read data for objects for {self.object_type}")

        df = (
            df.withColumns(self._cols_map)
            .select(*self._cols_map.keys())
        )

        return df

    def normalize_privilege_read(self, df: DataFrame, parent_object_overwrite: str = None) -> DataFrame:
        """
        Normalize the read data for privileges

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the read data for privileges for {self.object_type}")

        if parent_object_overwrite:
            df = (
                df.withColumns(self._cols_map)
                .groupBy(*(list(self._cols_map.keys()) + list(["grantee"])))
                .agg(F.concat_ws(", ", F.collect_list(F.col("privilege_type"))).alias("privilege_types"))
                .withColumn("restore_ddl", F.concat(F.lit("GRANT "),
                                                    F.col("privilege_types"),
                                                    F.lit(" ON "),
                                                    F.lit(parent_object_overwrite),
                                                    F.lit(" `"),
                                                    F.replace(F.col("object_name"), F.lit("."), F.lit("`.`")),
                                                    F.lit("` TO `"),
                                                    F.col("grantee"),
                                                    F.lit("`")))
                .withColumn("object_cross_relationship", F.col("grantee"))      
                .select(*self._cols_map.keys())
            )
        else:
            df = (
                df.withColumns(self._cols_map)
                .groupBy(*(list(self._cols_map.keys()) + list(["grantee"])))
                .agg(F.concat_ws(", ", F.collect_list(F.col("privilege_type"))).alias("privilege_types"))
                .withColumn("restore_ddl", F.concat(F.lit("GRANT "),
                                                    F.col("privilege_types"),
                                                    F.lit(" ON "),
                                                    F.col("object_parent_type"),
                                                    F.lit(" `"),
                                                    F.replace(F.col("object_name"), F.lit("."), F.lit("`.`")),
                                                    F.lit("` TO `"),
                                                    F.col("grantee"),
                                                    F.lit("`")))
                .withColumn("object_cross_relationship", F.col("grantee"))      
                .select(*self._cols_map.keys())
            )

        return df
    
    def normalize_tag_read(self, df: DataFrame) -> DataFrame:
        """
        Normalize the read data for tags

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the read data for tag for {self.object_type}")

        # TODO: review logic for optimization
        df = (
            df.withColumns(self._cols_map)
            .withColumn("tags", F.concat(F.lit("'"), F.col("tag_name"), F.lit("' = '"), F.col("tag_value"), F.lit("'")))
            .groupBy(*self._cols_map.keys())
            .agg(F.concat_ws(", ", F.collect_list(F.col("tags"))).alias("tags"))
            .withColumn("restore_ddl", F.concat(F.lit("ALTER "), 
                                                F.col("object_parent_type"),
                                                F.lit(" `"),
                                                F.replace(F.col("object_name"), F.lit("."), F.lit("`.`")),
                                                F.lit("` SET TAGS ("),
                                                F.col("tags"), 
                                                F.lit(")")))
            .withColumn("object_cross_relationship", F.col("tags"))            
            .select(*self._cols_map.keys())
        )

        return df
    
    def add_owner_object(self, df: DataFrame) -> DataFrame:
        """
        Add owner and object columns to the DataFrame

        Args:
            df (DataFrame): The DataFrame to add the owner and object columns to

        Returns:
            DataFrame: The DataFrame with the owner and object columns added
        """

        if not df.isEmpty():
            df_owner = (
                    df.withColumn("object_parent_name", F.col("object_name"))
                    .withColumn("object_parent_type", F.col("object_type"))
                    .withColumn("object_cross_relationship", F.col("object_owner"))
                    .withColumn("object_type", F.lit(ObjectType("OWNER").value))
            )

            if (self.object_type == ObjectType.REGISTERED_MODEL):
                self._logger.info("Adding owner and object columns in Registered Models")

                df_owner = (
                    df_owner.withColumn("restore_ddl", F.lit(None).cast(T.StringType())) # clean api payload content
                    .withColumn("api_payload", F.concat(F.lit("{ \"full_name\": \""), 
                                                                F.col("object_name"),
                                                                F.lit("\", \"owner\": \""), 
                                                                F.col("object_owner"), 
                                                                F.lit("\"}")))
                )

                df = df.select(*self._cols_map.keys()).union(df_owner)

            elif (     self.object_type != ObjectType.PRIVILEGE 
                   and self.object_type != ObjectType.TAG
                   and self.object_type != ObjectType.DELTA_SHARING_SHARE_OBJECT):
            
                self._logger.info("Adding owner and object columns")

                df_owner = (
                    df_owner.withColumn("api_payload", F.lit(None).cast(T.StringType())) # clean api payload content
                    .withColumn("restore_ddl", F.concat(F.lit("ALTER "), 
                                                                F.lit(self.object_type.value), 
                                                                F.lit(" `"), 
                                                                F.replace(F.col("object_name"), F.lit("."), F.lit("`.`")), 
                                                                F.lit("` SET OWNER TO `"), 
                                                                F.col("object_owner"), 
                                                                F.lit("`")))
                    
                )

                df = df.select(*self._cols_map.keys()).union(df_owner)

        # Remove the object_owner column, since it is not needed in the final output
        df = df.drop("object_owner")

        return df
    
    def add_uid(self, df: DataFrame) -> DataFrame:
        """
        Add uid column to the DataFrame

        Args:
            df (DataFrame): The DataFrame to add the uid column to

        Returns:
            DataFrame: The DataFrame with the uid column added
        """
        self._logger.info(f"Adding uid column for {self.object_type}")

        if not df.isEmpty():
            df = df.withColumn("object_uid", F.xxhash64(*df.columns))
        else:
            df = df.withColumn("object_uid", F.lit(None).cast(T.LongType()))

        return df
    
    def read_past_backup_data(self, df: DataFrame, latest_update: dt.datetime) -> DataFrame:
        """
        Read past backup data

        Args:
            df (DataFrame): The DataFrame to read past backup data from
            latest_update (datetime): The latest update timestamp

        Returns:
            DataFrame: The DataFrame with the past backup data read
        """
        self._logger.info(f"Reading past backup data for {self.object_type}")

        if (self.object_type == ObjectType.TABLE and self.backup_mode == BackupMode.INCREMENTAL_BACKUP):
            try:
                # Read past backup objects from source table (system table)
                source_table_df = (
                    self._spark.table(self._source_table)
                    .filter(F.col("last_altered") <= F.lit(latest_update))
                    .transform(self.pre_process)
                    .withColumn("object_name", self._cols_map["object_name"])
                    .select("object_name")
                )

                # Read past backed up objects from backup data
                backup_data_df = (
                    self._spark.read.load(self._dest_storage_path)
                    .filter(   (F.col("object_type") == F.lit(self._object_type.value)) 
                             | (   (F.col("object_parent_type") == F.lit(self._object_type.value))
                                 & (F.col("object_type") == F.lit(ObjectType.OWNER.value))))
                    .filter((F.col("restore_ddl").isNotNull()) | (F.col("api_payload").isNotNull())) # filter out objects without restore_ddl or api_payload
                )

                # Persist existing objects
                existing_objects_df = (
                    backup_data_df.alias("left").join(source_table_df.alias("right"), ["object_name"], "inner")
                    .select([f"left.{col}" for col in backup_data_df.columns])
                )

                df = df.union(existing_objects_df)
            except Exception as e:
                self._logger.warning(f"No past backup info to persist for {self.object_type}. {str(e)}")

        return df

    def _parallelize(self, function: Callable, objects: List[str], threads: int = 8, timeout: Optional[int] = None) -> DataFrame:
        """
        Parallelize the function

        Args:
            function (Callable): The function to parallelize
            objects (List[str]): The list of objects
            threads (int): The number of threads to use
            timeout (int, optional): The timeout. Defaults to None.

        Returns:
            List[str]: The list of results
        """

        result = []
        with ThreadPoolExecutor(max_workers=threads) as executor:

            for obj, future in [(object, executor.submit(function, object)) for object in objects]:
                try:
                    sync_status = future.result(timeout=timeout)
                    if isinstance(sync_status, Tuple):
                        result.append(sync_status)
                    elif isinstance(sync_status, List):
                        # Flatten list
                        result += [status for status in sync_status]
                except:
                    self._logger.warning(f"Error parallelizing function {function.__name__}")
                    result.append(tuple([obj, None]))

        return self._spark.createDataFrame(result, "object_name string, restore_ddl string")
    
    def _validate_cols(self, cols: List[str]) -> None:
        """
        Validate the columns

        Args:
            cols (List[str]): The columns to validate
        """
        self._logger.info(f"Validating the columns for {self.object_type}")

        # convert to list
        cols = list(cols)

        if "object_owner" in cols:
            # Not part of final schema
            cols.remove("object_owner")

        # it is part of the final schema and it is added within the class
        cols.append("created_tsp")

        if sorted(cols) != sorted(self._schema.fieldNames()):
            self._logger.error(f"The columns do not match the schema. Input cols: {cols}. Expected Cols: {self._schema.fieldNames()}")
            raise ValueError("The columns do not match the schema")
        
    def _update_cols_map(self, cols_map: Dict) -> None:
        """
        Update the columns map

        Args:
            cols_map (Dict): The columns map to update
        """
        self._logger.info("Updating the columns map")

        self._cols_map = cols_map
        self._validate_cols(cols_map.keys())

    def _sql_stmt_with_retry(
        self,
        sql_stmt: str,
        retries: Optional[int] = 3,
        delay: int = 1,
        retry_callback: Optional[Callable[..., None]] = None
    ) -> DataFrame:
        """
        Execute SQL statement with retry mechanism

        Args:
            sql_stmt (str): SQL statement to be executed
            retries (int, optional): Number of retries. Defaults to RETRIES.
            delay (int): Delay between retries. Defaults to 10.
            retry_callback (Optional[Callable[...]], optional): Callback function to be executed when SQL statement fails. Defaults to None.

        Returns:
            None
        """

        retries = self.retries if retries is None else retries

        for i in range(retries):
            try:
                return self._spark.sql(sql_stmt)
            except Exception as e:
                msg = f"SQL Stamement failed. Error: {str(e)}"
                if i < retries - 1:
                    self._logger.debug(msg)
                    if retry_callback is not None:
                        self._logger.debug(f"Executing retry callback function...")
                        retry_callback(e)
                    else:
                        self._logger.debug(f"Retrying in {delay} seconds...")
                        time.sleep(delay)
                else:
                    self._logger.error(f"Failed max times to execute sql statement: {sql_stmt}")
                    raise e