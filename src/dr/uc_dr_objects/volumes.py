from dr.uc_dr_objects.objects import Objects, ObjectType, NON_UC_USER_CATALOGS_FOR_TABLES, NON_UC_USER_SCHEMAS_FOR_TABLES
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Column


class Volumes(Objects):
    """
    This class is responsible for reading and writing UC Volumes objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """
        super().__init__(ObjectType.VOLUME, "system.information_schema.volumes")

        self._update_cols_map({
            "object_name": F.col("volume_name"),
            "object_owner": F.col("volume_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.concat(F.col("volume_catalog"), F.lit("."), F.col("volume_schema")),
            "object_parent_type": F.lit(ObjectType.SCHEMA.value),
            "object_cross_relationship": F.col("volume_name"),
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
            df.filter(F.col("volume_type").isin(["EXTERNAL", "MANAGED"]))
            .withColumn("volume_name", F.concat(F.col("volume_catalog"), F.lit("."), F.col("volume_schema"), F.lit("."), F.col("volume_name")))
        )
    
        return df

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
            "volume_name": f"`{cols.volume_name.replace('.', '`.`')}`",
            "type": cols.volume_type,
            "storage_location": cols.storage_location,
            "comment": cols.comment,
        }

        volume_ddl = DDL(ObjectType.VOLUME)

        return volume_ddl(**kwargs)
    

class VolumePrivileges(Objects):
    """
    This class is responsible for reading and writing UC volume privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.volume_privileges")

        self._update_cols_map({
            "object_name": F.col("volume_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("volume_name"),
            "object_parent_type": F.lit(ObjectType.VOLUME.value),
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
            .withColumn("volume_name", F.concat(F.col("volume_catalog"), F.lit("."), F.col("volume_schema"), F.lit("."), F.col("volume_name")))
        )

        return df
    

class VolumeTags(Objects):
    """
    This class is responsible for reading and writing UC Voluma Tags objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """
        super().__init__(ObjectType.TAG, "system.information_schema.volume_tags")

        self._update_cols_map({
            "object_name": F.col("volume_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.TAG.value),
            "object_parent_name": F.col("schema_name"),
            "object_parent_type": F.lit(ObjectType.VOLUME.value),
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
            df.filter(~F.col("catalog_name").isin(NON_UC_USER_CATALOGS_FOR_TABLES))
            .filter(~F.col("schema_name").isin(NON_UC_USER_SCHEMAS_FOR_TABLES))
            .withColumn("volume_name", F.concat(F.col("catalog_name"), F.lit("."), F.col("schema_name"), F.lit("."), F.col("volume_name")))
        )

        return df