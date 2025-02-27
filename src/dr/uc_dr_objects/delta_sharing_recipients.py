from dr.uc_dr_objects.objects import Objects, ObjectType
from dr.uc_dr_objects.ddl import DDL
from pyspark.sql import DataFrame
from pyspark.sql import Column
from pyspark.sql import functions as F
from pyspark.sql import types as T


class DSRecipients(Objects):
    """
    This class is responsible for reading and writing UC DS Recipient objects for Disaster Recovery
    """

    def __init__(self, threads: int = 8) -> None:
        """
        Initializes the class
        """

        super().__init__(ObjectType.DELTA_SHARING_RECIPIENT, "system.information_schema.recipients")
        
        self._threads = threads
        self._update_cols_map({
            "object_name": F.col("recipient_name"),
            "object_owner": F.col("recipient_owner"), # only needed because of the set ownership command
            "object_type": F.lit(self.object_type.value),
            "object_parent_name": F.lit(self.metastore_id),
            "object_parent_type": F.lit(ObjectType.METASTORE.value),
            "object_cross_relationship": F.col("recipient_name"),
            "object_uid": F.lit(None).cast(T.LongType()),
            "restore_ddl": F.col("restore_ddl"),
            "api_payload": F.col("api_payload")
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

        # Delta Sharing Recipients have an allow IP list
        allow_ip_range_df = (
            self._spark.table("system.information_schema.recipient_allowed_ip_ranges")
            .withColumn("allowed_ip_range", F.when(F.col("allowed_ip_range").isNotNull(), F.concat(F.lit("\""), 
                                                                                                   F.col("allowed_ip_range"), 
                                                                                                   F.lit("\"")))
                                            .otherwise(F.col("allowed_ip_range")))
            .groupBy("recipient_name").agg(
                F.concat(F.lit("{ \"ip_access_list\": { \"allowed_ip_addresses\": [ "), 
                         F.concat_ws(", ", F.collect_list(F.col("allowed_ip_range"))),
                         F.lit(" ]}}")).alias("api_payload"))
        )

        if not allow_ip_range_df.isEmpty():
            df = df.join(allow_ip_range_df, "recipient_name", "left")
        else:
            df = df.withColumn("api_payload", F.lit(None).cast(T.StringType()))

        df = df.withColumn("restore_ddl", self.ddl_conversion(F.struct(df.columns)))

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
            "recipient_name": cols.recipient_name,
            "delta_sharing_id": cols.data_recipient_global_metastore_id if cols.data_recipient_global_metastore_id not in (None, "") else False,
            "comment": cols.comment,
        }

        recipient_ddl = DDL(ObjectType(ObjectType.DELTA_SHARING_RECIPIENT))

        return recipient_ddl(**kwargs)