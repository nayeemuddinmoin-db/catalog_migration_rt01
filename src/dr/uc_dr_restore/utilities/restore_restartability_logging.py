from pyspark.sql import DataFrame
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType,LongType
from delta.tables import *
from pyspark.sql.functions import col,lit
from pyspark.sql import SparkSession
import logging
from pyspark.sql.utils import AnalysisException

log_level = logging.INFO

logging.basicConfig(
    format="%(asctime)s.%(msecs)03d [%(filename)s:%(lineno)d] - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)

# Create SparkSession
spark = SparkSession.builder \
    .appName("dr_restore") \
    .getOrCreate()



batch_control_schema = "jobid BIGINT, rowid STRING, status STRING, error_message STRING, record_timestamp TIMESTAMP"
batch_control_df = spark.createDataFrame([], batch_control_schema)

# Function to append records to the DataFrame
def append_record(batch_job_control_table_name:str, job_id:int, rowid:str) -> None:
    global batch_control_df
    job_control_table_schema = ["jobid","rowid","status", "record_timestamp"]
    data = [(int(f'{job_id}'), f'{rowid}', 'Succeeded', datetime.now())]
    df = spark.createDataFrame(data, schema=job_control_table_schema)
    batch_control_df = batch_control_df.unionAll(df)
    # return batch_control_df


def insert_and_reset_batch_job_control_execution(batch_job_control_table_name:str, df:DataFrame) -> None:
    global batch_control_df
    df.write.mode("append").saveAsTable(batch_job_control_table_name)
    
    batch_control_df = spark.createDataFrame([], batch_control_schema)

    # return df


def insert_batch_job_control_execution(df:DataFrame, batch_job_control_table_path:str, job_id:int) -> None:
    MAX_RETRIES = 2
    num_retries = 0
    while True:
        try:
            (   df
                .selectExpr(f"cast({job_id} as long) as jobid","object_uid as rowid", "status", "error_message", "current_timestamp as record_timestamp")
                .write.mode("append").format("delta")
            .save(batch_job_control_table_path)
            )
            break # exit the loop
        except Exception as e:
            if num_retries > MAX_RETRIES:
                raise e
            else:
                print("Retrying ...")
                num_retries += 1


# def insert_batch_job_control_execution(batch_job_control_table_name:str, job_id:int, rowid:str) -> int:
#     # job_control_table_schema = get_job_control_table_schema()
#     job_control_table_schema = ["jobid","rowid","status", "record_timestamp"]
#     data = [(int(f'{job_id}'), f'{rowid}', 'Succeeded', datetime.now())]
#     df = spark.createDataFrame(data, schema=job_control_table_schema)
#     df.write.mode("append").saveAsTable(batch_job_control_table_name)


def get_job_table_schema() -> StructType:
    # Define the schema for the batch job execution records
    schema = StructType([
        StructField("jobid", LongType(), False),
        StructField("jobtype", StringType(), False),
        StructField("jobstatus", StringType(), False),
        StructField("jobstart", TimestampType(), False),
        StructField("jobend", TimestampType(), True),
        StructField("backupversion", IntegerType(), False)
    ])
    return schema


def get_max_job_id(batch_table_path:str, job_type:str) -> int:
    return spark.sql(f"select coalesce(max(jobid), 0) as max_jobid from delta.`{batch_table_path}` where jobtype='{job_type}'").first()[0]

def get_max_job_status(batch_table_path:str, job_type:str) -> str:
    max_job_id = get_max_job_id(batch_table_path, job_type)
    return spark.sql(f"select jobstatus from delta.`{batch_table_path}` where jobtype='{job_type}' and jobid={max_job_id}").first()[0]

def get_max_batch_job_control_id(batch_job_control_table_name:str) -> int:
    return spark.sql(f"select coalesce(max(batchjobcontrolid), 0) as max_batchjobcontrolid from {batch_job_control_table_name}").first()[0]


def insert_batch_execution(batch_table_path:str, job_type:str, job_status:str, backupversion:int) -> None:
    batch_table_schema = get_job_table_schema()
    max_jobid = get_max_job_id(batch_table_path, job_type) + 1
    data = [(int(f'{max_jobid}'), f'{job_type}', f'{job_status}', datetime.now(), None, int(f'{backupversion}'))]
    df = spark.createDataFrame(data, schema=batch_table_schema)
    logger.info("Creating a new record in BatchJob table with JobId = %s" %max_jobid)
    df.write.mode("append").format("delta").save(batch_table_path)
    




def update_batch_execution_status(batch_table_path:str, job_type:str, current_job_status:str='Running,Failed', future_job_status:str='Completed', jobend:TimestampType=datetime.now()):
    max_job_id = get_max_job_id(batch_table_path, job_type)
    deltaTable = DeltaTable.forPath(spark, batch_table_path)
    # deltaTable.update(f"jobstatus in '{current_job_status}' and jobtype = '{job_type}' and jobid = {max_job_id}", { "jobstatus": f"'{future_job_status}'", "jobend":f"'{jobend}'"  } )
    if future_job_status == 'Running':
        # deltaTable.update(f"jobstatus = '{current_job_status}' and jobtype = '{job_type}' and jobid = {max_job_id}", { "jobstatus": f"'{future_job_status}'" } )
        deltaTable.update(f"jobtype = '{job_type}' and jobid = {max_job_id}", { "jobstatus": f"'{future_job_status}'" } )
    else:
        deltaTable.update(((col("jobstatus").isin(current_job_status.split(','))) & (col("jobtype") == f'{job_type}') & (col("jobid") == f'{max_job_id}')), { "jobstatus": lit(future_job_status), "jobend": lit(jobend)  } )


def get_table_version_from_timestamp(backup_table_path:str, timestamp_value:str) -> int:
    df_version_query = spark.sql(f"""with src as (select version, 
                                    timestamp, 
                                    lag(timestamp) over(order by version desc) next_run_timestamp  
                                    from (
                                        desc history delta.`{backup_table_path}`))
                                        select version from src
                                    where '{timestamp_value}' >= timestamp and '{timestamp_value}' < next_run_timestamp
                                """)
    
    return df_version_query.first().version


def get_latest_table_version(backup_table_path:str) -> int:
    df_version_query = spark.sql(f"desc history delta.`{backup_table_path}`")

    return df_version_query.first().version