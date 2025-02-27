from pyspark.sql import SparkSession
import json
import re


"""
**************************************************************************************************
Validate JSON structure based on the input json to restore all or specific set of UC objects
**************************************************************************************************
"""

def validate_json_structure(data):
    try:     
        # Check for the presence of required keys
        if "metastore" not in data:
            print("Key 'metastore' is missing.")
            return False

        # Ensure metastore is a dictionary
        if data["metastore"] != "*":
            if not isinstance(data["metastore"], dict):
                print("Value of 'metastore' key should be a dictionary.")
                return False

        # Validate metastore objects
        if data["metastore"] != "*":
            metastore_objects = ["storage credential", "share", "recipient", "connection", "external location"]
            for obj in metastore_objects:
                if obj in data["metastore"]:
                    if data["metastore"][obj] != "*":
                        if not isinstance(data["metastore"][obj], list):
                            print(f"Value of '{obj}' should be a list or it can be '*'")
                            return False

                    for item in data["metastore"][obj]:
                        if not isinstance(item, str):
                            print(f"Elements of '{obj}' should be strings.")
                            return False

                else:
                    # Ensure no other keys are present outside specified objects
                    for key in data["metastore"]:
                        if key not in metastore_objects and key != "catalog":
                            print(f"Invalid key '{key}' found outside specified objects.")
                            return False

        # Validate catalogs
        if "catalog" in data["metastore"]:
            catalogs = data["metastore"]["catalog"]
            if catalogs != "*":
                if not isinstance(catalogs, dict):
                    print("Value of 'catalog' key should be a dictionary or it can be '*'")
                    return False
                for catalog_name, catalog_data in catalogs.items():
                    if catalog_data != "*":
                        if not isinstance(catalog_data, dict):
                            print(f"Value of catalog '{catalog_name}' should be a dictionary.")
                            return False
                        # Validate schemas
                        if "schema" in catalog_data.keys():
                            schema = catalog_data["schema"]
                            if schema != "*":
                                if not isinstance(schema, dict):
                                    print(f"Value of 'schema' under catalog '{catalog_name}' should be a dictionary.")
                                    return False

                                for schema_name, schema_data in schema.items():
                                        if schema_data != "*":
                                            if not isinstance(schema_data, dict):
                                                print(f"Value of schema '{schema_name}' under catalog '{catalog_name}' should be a dictionary.      ")
                                                return False

                                            # Validate child objects
                                            for obj_type, obj_list in schema_data.items():
                                                schema_child_objects = ["table","view","volume","function","registered model"]
                                                if obj_type in schema_child_objects:
                                                    if obj_list != "*":
                                                        if not isinstance(obj_list, list):
                                                            print(f"Value of '{obj_type}' under schema '{schema_name}' and catalog '            {catalog_name}  '     should be a list.")
                                                            return False
                                                        # If regex patterns are provided, validate them
                                                        if obj_type in ["table","view","volume","function","registered model"]:
                                                            for obj_pattern in obj_list:
                                                                if obj_pattern != "*":
                                                                    try:
                                                                        re.compile(obj_pattern)
                                                                    except re.error:
                                                                        # Invalid regex pattern
                                                                        print(f"Invalid regex pattern '{obj_pattern}' for '{obj_type}' under        schema '{schema_name}' and catalog '{catalog_name}'.")
                                                                        return False
                                                else:
                                                    print(f"""Invalid Schema child key '{obj_type}' found in schema {schema_name} which is outside of possible objects.""")
                                                    return False
                        else:
                            for s in catalog_data.keys():
                                if s != "schema":
                                    print(f"Invalid Catalog child key '{s}' found outside possible key.")
                                    return False
        # If all checks pass, return True
        return True
    except json.JSONDecodeError:
        # If there's an error decoding JSON, return False
        print("Error decoding JSON.")
        return False



"""
**************************************************************************************************
Build Filter condition based on the input json to restore all or specific set of UC objects
**************************************************************************************************
"""


def build_filter_conditions(data : dict,backup_table_adls_path:str, spark:SparkSession, time_travel_option:str, time_travel_value:str) -> str:

    # Initialize filter string
    filter_str = ""

    # Handle metastore objects
    metastore = data.get("metastore", {})
    if metastore == "*":
        filter_str += "(lit(1) == lit(1))"
        return filter_str

    for obj, value in metastore.items():
        # print(obj,value)
        if obj == 'storage credential':
            if value == "*":
                filter_str += f" ((col('object_type') == '{obj.upper()}') | (col('object_type') == 'EXTERNAL LOCATION')) |"
            else:
                filter_str += f" (((col('object_type') == '{obj.upper()}') &  (col('object_name').isin({value}))) | ((col('object_type') == 'EXTERNAL LOCATION') &  (col('object_parent_name').isin({value})))) |"
        elif obj != 'catalog':
            if value == "*":
                filter_str += f" (col('object_type') == '{obj.upper()}') |"
            else:
                filter_str += f" ((col('object_type') == '{obj.upper()}') &  (col('object_name').isin({value}))) |"
        elif obj == 'catalog':
            if value == "*":
                filter_str += f" (col('object_type') == '{obj.upper()}') |"
                filter_str += f" ( (col('object_type') == 'SCHEMA') | (col('object_parent_type') == 'SCHEMA') ) |"
            else:
                filter_str += f"((col('object_type') == '{obj.upper()}') &  (col('object_name').isin({list(value.keys())}))) | "
                for catalog_name, catalog_child in value.items():
                    print(catalog_name, catalog_child)
                    # filter_str += f"((col('object_type') == '{obj.upper()}') &  (col('object_name').isin({value.keys()})))"
                    if catalog_child == '*':
                        filter_str += f""" ( (col('object_parent_type') == '{obj.upper()}') & (col('object_parent_name') == '{catalog_name}')
                                                     & (col('object_type')     == 'SCHEMA') ) | """
    
                        if time_travel_value in ("", None):
                            df = spark.read.format("delta").load(backup_table_adls_path)
                        elif time_travel_option.lower() == "version":
                            df = spark.read.format("delta").option("versionAsOf", time_travel_value).load(backup_table_adls_path)
                        elif time_travel_option.lower() == "timestamp":
                            df = spark.read.format("delta").option("timestampAsOf", time_travel_value).load(backup_table_adls_path)
                        schema_names = df.where(f"object_type='SCHEMA' and object_parent_name = '{catalog_name}'").select("object_name").collect()
                        schema_names_list = [s['object_name'] for s in schema_names]
                        # print(schema_names_list)
    
                        filter_str += f""" ( (col('object_parent_type') == 'SCHEMA') & (col('object_parent_name').isin({schema_names_list}))) |"""

                    else:
                        for schema, schema_name in catalog_child.items():
                            # print(schema, schema_name)
                            if schema_name == "*":
                                filter_str += f""" ( (col('object_parent_type') == '{obj.upper()}') & (col('object_parent_name') == '{catalog_name}')
                                                     & (col('object_type')     == '{schema.upper()}') ) | """
    
                                if time_travel_value in ("", None):
                                    df = spark.read.format("delta").load(backup_table_adls_path)
                                elif time_travel_option.lower() == "version":
                                    df = spark.read.format("delta").option("versionAsOf", time_travel_value).load(backup_table_adls_path)
                                elif time_travel_option.lower() == "timestamp":
                                    df = spark.read.format("delta").option("timestampAsOf", time_travel_value).load(backup_table_adls_path)
                                schema_names = df.where(f"object_type='{schema.upper()}' and object_parent_name = '{catalog_name}'").select("object_name").collect()
                                schema_names_list = [s['object_name'] for s in schema_names]
                                print(schema_names_list)
    
                                filter_str += f""" ( (col('object_parent_type') == '{schema.upper()}') & (col('object_parent_name').isin({schema_names_list})) ) |"""
    
                            else:
                                for schema_name, schema_child in schema_name.items():
                                    # print(schema_name, schema_child)
                                    if schema_child == "*":
                                        filter_str += f""" ( (col('object_parent_type') == '{obj.upper()}') & (col('object_parent_name') == '{catalog_name}') & (col('object_type')     == '{schema.upper()}') & (col('object_name') == '{schema_name}') ) |"""
    
                                        filter_str += f" ((col('object_parent_type') == '{schema.upper()}') &  (col('object_parent_name') == '{schema_name}')) |"
    
                                    else:
                                        filter_str += f""" ( (col('object_parent_type') == '{obj.upper()}') & (col('object_parent_name') == '{catalog_name}') & (col('object_type')     == '{schema.upper()}') & (col('object_name') == '{schema_name}') ) |"""
                                        for schema_child, schema_child_name in schema_child.items():
                                            # print(schema_child, schema_child_name)
                                            if schema_child_name == "*":
                                                filter_str += f""" ((col('object_parent_type') == '{schema.upper()}') &  
                                                (col('object_parent_name') == '{schema_name}') & (col('object_type') == '{schema_child.upper()}')) |"""
                                            else:
                                                filter_str += f" ((col('object_parent_type') == '{schema.upper()}') &  (col('object_parent_name') == '{schema_name}') & (col('object_type') == '{schema_child.upper()}') &  (col('object_name').isin({schema_child_name}))) |"
    
    return "("+filter_str.rstrip("|")+")"


"""
**************************************************************************************************
This function provides an option to do a dry run before the actual execution. Instead of executing
**************************************************************************************************
"""

def execute_spark_sql(query_str:str, dry_run:bool=False):
    # Create SparkSession object
    spark = SparkSession.builder.getOrCreate()

    if dry_run:
        print(query_str)
        return spark.createDataFrame([], schema="col STRING")
    else:
        return spark.sql(query_str)