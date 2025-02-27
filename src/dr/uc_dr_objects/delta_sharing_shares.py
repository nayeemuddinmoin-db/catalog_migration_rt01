from dr.uc_dr_objects.objects import Objects, ObjectType
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T
from typing import Tuple


class DSShares(Objects):
    """
    This class is responsible for reading and writing UC DS Shares objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.DELTA_SHARING_SHARE, "system.information_schema.shares")
        
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("share_name"),
            "object_owner": F.col("share_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.lit(self.metastore_id),
            "object_parent_type": F.lit(ObjectType.METASTORE.value),
            "object_cross_relationship": F.col("share_name"),
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
            "share_name": cols.share_name,
            "comment": cols.comment,
        }

        share_ddl = DDL(ObjectType(ObjectType.DELTA_SHARING_SHARE))

        return share_ddl(**kwargs)


class DSShareRecipientPrivileges(Objects):
    """
    This class is responsible for reading and writing UC External Locations privileges objects for Disaster Recovery
    """

    def __init__(self) -> None:
        """
        Initializes the class
        """
        super().__init__(ObjectType.PRIVILEGE, "system.information_schema.share_recipient_privileges")

        self._update_cols_map({
            "object_name": F.col("share_name"),
            "object_owner": F.lit(None).cast(T.StringType()),
            "object_type": F.lit(ObjectType.PRIVILEGE.value),
            "object_parent_name": F.col("share_name"),
            "object_parent_type": F.lit(ObjectType.DELTA_SHARING_SHARE.value),
            "object_cross_relationship": F.lit(None).cast(T.StringType()), # Will be based on privileges
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.lit(None).cast(T.StringType()),
            "api_payload": F.lit(None).cast(T.StringType())
        })

    def normalize_privilege_read(self, df: DataFrame) -> DataFrame:
        """
        Normalize the read data for privileges

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the privileges data for {self.object_type}")

        df = (
            df.groupBy("share_name", "recipient_name")
            .agg(F.concat_ws(", ", F.collect_list(F.col("privilege_type"))).alias("privilege_types"))
            .withColumns(self._cols_map)
            .filter((F.col("object_name").isNotNull()) & (F.col("object_name") != "") & (F.col("recipient_name").isNotNull()) & (F.col("recipient_name") != ""))
            .withColumn("restore_ddl", F.concat(F.lit("GRANT "), 
                                                F.col("privilege_types"), 
                                                F.lit(" ON "),
                                                F.col("object_parent_type"),
                                                F.lit(" `"), 
                                                F.col("object_name"), 
                                                F.lit("` TO RECIPIENT `"), 
                                                F.col("recipient_name"), 
                                                F.lit("`")))                
            .select(*self._cols_map.keys())
        )

        return df


class DSShareObjects(Objects):
    """
    This class is responsible for reading and writing UC DS Share Objects objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.DELTA_SHARING_SHARE_OBJECT, "system.information_schema.shares")

        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("share_name"),
            "object_owner": F.col("share_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.col("share_name"),
            "object_parent_type": F.lit(ObjectType.DELTA_SHARING_SHARE.value),
            "object_cross_relationship": F.regexp_extract(F.col("restore_ddl"), r"^(?i)alter share [^\n]+\nadd (?:materialized view|\w+) ([^($|\s)]+)", 1),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.col("restore_ddl"),
            "api_payload": F.lit(None).cast(T.StringType())
        })
    
    def generate_restore_stmt(self, df: DataFrame) -> DataFrame:
        """
        Normalize the read data

        Args:
            df (DataFrame): The DataFrame to normalize

        Returns:
            DataFrame: The normalized DataFrame
        """
        self._logger.info(f"Normalizing the read data for {self.object_type}")

        share_list = list(map(lambda x: x[0], df.select("share_name").distinct().collect()))
        share_ddl_df = self._parallelize(self.ddl_conversion, share_list, self._threads)

        df = (
            df.join(share_ddl_df, [df.share_name == share_ddl_df.object_name], "left")
            .filter(F.col("restore_ddl").isNotNull())
        )

        return df

    def ddl_conversion(self, share: str) -> Tuple[str, str]:
        """
        Get the DDL for the tables
        
        Args:
            table (str): The table name
            
        Returns:
            Tuple[str, str]: The table name and the DDL
        """
        self._logger.info(f"Getting the DDL for the DS Share Objects from Share {share}")

        share_details = []
        try:
            rows = self._sql_stmt_with_retry(f"SHOW ALL IN SHARE `{share}`").collect()

            for row in rows:
                kwargs ={
                    "share_name": share,
                    row.type.lower().replace(" ", "_"): f"`{row.shared_object.replace('.', '`.`')}`" if row.shared_object else None,
                    # "schema": row.shared_object if row.type.lower() == "schema" else None,
                    # "table": row.shared_object if row.type.lower() == "table" else None,
                    # "mat_view": row.shared_object if row.type.lower() == "materialized view" else None,
                    # "view": row.shared_object if row.type.lower() == "view" else None,
                    # "model": row.shared_object if row.type.lower() == "model" else None,
                    "comment": row.comment,
                    "alias": row.name,
                    "partition": row.partitions,
                    "history": row.history_sharing.lower() != "disabled"
                }

                share_details.append(tuple([share, DDL(self.object_type)(**kwargs)]))
            
            self._logger.debug(f"Share Details: {share_details}")

        except Exception as e:
            self._logger.warning(f"Error getting the DDL for the tables: {e}")
            return [(share, None)]

        return share_details