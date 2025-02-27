from dr.uc_dr_objects.objects import Objects, ObjectType
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Column


class ExternalLocations(Objects):
    """
    This class is responsible for reading and writing UC External Location objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class

        Args:
            threads (int): The number of threads to use for parallel processing
        """

        super().__init__(ObjectType.EXTERNAL_LOCATION, "system.information_schema.external_locations")
        
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("external_location_name"),
            "object_owner": F.col("external_location_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.col("storage_credential_name"),
            "object_parent_type": F.lit(ObjectType.STORAGE_CREDENTIAL.value),
            "object_cross_relationship": F.col("external_location_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.col("restore_ddl"),
            "api_payload": F.when(F.col("read_only") == F.lit(True), F.lit("{ \"read_only\": true }"))
                            .otherwise(F.lit(None).cast(T.StringType()))
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
            "external_location_name": cols.external_location_name,
            "url": cols.url,
            "storage_credential_name": cols.storage_credential_name,
            "comment": cols.comment
        }

        schema_ddl = DDL(ObjectType.EXTERNAL_LOCATION)

        return schema_ddl(**kwargs)


class ExternalLocationPrivileges(Objects):
    """
    This class is responsible for reading and writing UC External Locations privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.external_location_privileges")

        self._update_cols_map({
            "object_name": F.col("external_location_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("external_location_name"),
            "object_parent_type": F.lit(ObjectType.EXTERNAL_LOCATION.value),
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

        df = df.filter(F.col("inherited_from") == F.lit("NONE"))

        return df
