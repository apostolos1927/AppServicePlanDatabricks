# Databricks notebook source
import json
import os
from delta import DeltaTable
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType,StringType,DoubleType
from pyspark.sql import SparkSession


class Merge:
    def __init__(self,schema: Dict[str, str],primary_key: List[str],partition_key: str,output_dir: str):
        self.schema = schema
        self.spark = SparkSession.builder.getOrCreate()
        self.primary_key = primary_key
        self.partition_key = partition_key
        self.output_dir = output_dir


    def _overwrite_output_schema(self) -> None:
        df = self.spark.createDataFrame([], schema=self.schema)
        df.write.option("overwriteSchema", "True").partitionBy(self.partition_key).mode("append").saveAsTable(self.output_dir)

        

    def _merge_condition(self, target_table: str = "target", source_table: str = "source") -> str:
        conditions = [f"{target_table}.{column} = {source_table}.{column}" for column in self.primary_key]
        conditions_string = " AND ".join(conditions)
        return conditions_string




    def _when_matched(self, columns: List[str], target_table: str = "target", source_table: str = "source") -> str:
        columns = [f"{target_table}.{column} = {source_table}.{column}" for column in columns if column not in self.primary_key]
        conditions_string = ", ".join(columns)
        return conditions_string




    def _when_not_matched(self, columns: List[str], source_table: str = "source") -> str:
        table_columns = ", ".join(columns)
        values = ", ".join([f"{source_table}.{column}" for column in columns])
        statement = f"({table_columns}) VALUES ({values})"
        return statement




    def execute(self, df: DataFrame) -> None:

        #if there is no target table the first time then create an empty dataframe
        if not self.spark.catalog.tableExists(self.output_dir):
                self._overwrite_output_schema()
        #load target table

        #df_target = spark.read.format("delta").load(self.output_dir)

        df_target = self.spark.table(self.output_dir)
        df_target.display()
        df.createOrReplaceTempView("source")
        df_target.createOrReplaceTempView("target")

        columns = [i["name"] for i in df.schema.jsonValue()["fields"]]

        print(f"""
            MERGE INTO target
            USING source
            ON {self._merge_condition()}
            WHEN MATCHED THEN
                UPDATE SET {self._when_matched(columns)}
            WHEN NOT MATCHED
                THEN INSERT {self._when_not_matched(columns)}
            """)

        self.spark.sql(
            f"""
            MERGE INTO target
            USING source
            ON {self._merge_condition()}
            WHEN MATCHED THEN
                UPDATE SET {self._when_matched(columns)}
            WHEN NOT MATCHED
                THEN INSERT {self._when_not_matched(columns)}
            """
        )






class Changes:

    """

    Determine changes that happen on a delta table to only process changed data.

    The purpose of this class is to output a list of partition values (DateKey, WeekKey, etc.)

    that have changed data.

    """
    log_schema = StructType(
        [StructField("Version", IntegerType()), StructField("EventDateTime", TimestampType())]
    )

    determine_modes = ["merge", "append"]  ##add "replaceWhere" if you want to implement partition exchange

    def __init__(self, input_dir: str, log_dir: str, partition_key: list = None) -> None:

        self.input_dir = input_dir
        self.log_dir = log_dir
        self.changed_partition_keys: List[int] = []
        self.partition_key = partition_key
        self.spark = SparkSession.builder.getOrCreate()
        # Retrieve the current version of the source delta table

        self.new_delta_version = (
            self.spark.sql(f"DESCRIBE HISTORY {self.input_dir}")
            .where((col("operation") == "WRITE") | (col("operation") == "MERGE"))
            .agg(spark_max(col("version")).alias("MaxVersion"))
            .first()["MaxVersion"]
        )

        print('self.new_delta_version ====',self.new_delta_version)
        # Current version of the table in the log directory. Keep track of changes in the source table
        try:
            self.table_version = (
                self.spark.read.format("delta")
                .load(self.log_dir)
                .orderBy(col("EventDateTime").desc())
                .first()["Version"]
            )

            

        except Exception:
            # No log entry means retrieve the entire delta table.
            self.table_version = -1
        print('self.table_version =================',self.table_version)
        print('self.new_delta_version < self.table_version',self.new_delta_version < self.table_version)
        if self.new_delta_version < self.table_version:
            # In case of a table deletion, delta versioning starts at 1 again
            self.table_version = -1

    def _get_all_partitions(self) -> list:
        return [self.spark.table(self.input_dir).select(self.partition_key).distinct().collect()]

    def _generate_path(self, n: str) -> str:
        """For a given delta version, generate the file name for the commit file."""
        return os.path.join('/user/hive/warehouse/',self.input_dir.lower(), "_delta_log/", str(n).zfill(20) + ".json")

    def _determine_merge_append(self) -> list:
        if self.table_version == -1:
            return self._get_all_partitions()
        else:
            new_versions = [
                i[0]
                for i in self.spark.sql(f"DESCRIBE HISTORY {self.input_dir}")
                .where(col("version") > self.table_version)
                .where(
                    (col("operation") == "WRITE")
                    | (col("operation") == "MERGE")
                    | (col("operation") == "Append")
                )
                .select(col("version"))
                .distinct()
                .collect()
            ]
            if len(new_versions) > 0:

                # get the delta log files that show the delta changes

                # these delta log files contain the changed partitions
                delta_log_files = [self._generate_path(version) for version in new_versions]
                df_delta_log_files = self.spark.read.json(delta_log_files)
                columns_list = df_delta_log_files.columns

                

                # return the partition values of changed files

                if "add" in columns_list:

                    return (
                        [df_delta_log_files
                        .where("add is not null")
                        .where(f"add.partitionValues.{self.partition_key[0]} IS NOT NULL")
                        .select([f"add.partitionValues.{i}" for i in self.partition_key])
                        .distinct()
                        .collect()]
                    )

                else:
                    return []
            else:
                return []

    def determine(self, load_from: int = None, mode: str = "merge") -> list:
        """

        Determine all the partition values that have changed.

        """
        if isinstance(load_from, int):
            load_from = [str(load_from)]

        if mode not in self.determine_modes:
            raise ValueError(f"param mode should be one of the following values: {self.determine_modes}")

        if mode in ["append", "merge"]:
            changed_partitions = [i for i in self._determine_merge_append() if "".join([str(v) for v in i]) >= "".join(load_from)]
            self.changed_partition_keys = [v for i in changed_partitions for v in i ]

        return self.changed_partition_keys

    def create_log_record(self) -> None:
        df = self.spark.createDataFrame([[self.new_delta_version, datetime.now()]], self.log_schema)
        df.write.format("delta").mode("append").save(self.log_dir)


# COMMAND ----------

dbutils.widgets.dropdown("bronze_table", "BRONZE_CPU_PERCENTAGE",
                         ["BRONZE_CPU_PERCENTAGE", "BRONZE_MEMORY_PERCENTAGE","BRONZE_DISK_PERCENTAGE","BRONZE_DATA_IN_BYTES","BRONZE_DATA_OUT_BYTES"], "bronze_table")
bronze_table= dbutils.widgets.get("bronze_table")
print(bronze_table)

dbutils.widgets.dropdown("silver_table", "SILVER_CPU_PERCENTAGE",
                         ["SILVER_CPU_PERCENTAGE", "SILVER_MEMORY_PERCENTAGE","SILVER_DISK_PERCENTAGE","SILVER_DATA_IN_BYTES","SILVER_DATA_OUT_BYTES"], "silver_table")
silver_table= dbutils.widgets.get("silver_table")
print(silver_table)

dbutils.widgets.dropdown("prefix", "CPU",
                         ["CPU", "MEMORY","DISK","DATA_IN","DATA_OUT"], "prefix")
prefix= dbutils.widgets.get("prefix")
print(prefix)

dbutils.widgets.dropdown("metric_column", "average",
                         ["average", "total"], "metric_column")
metric_column= dbutils.widgets.get("metric_column")
print(metric_column)

upper_metric_column = metric_column.upper()

# COMMAND ----------

import requests
import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pyspark.sql import functions as F
from datetime import date, timedelta
from datetime import datetime
from pyspark.sql.types import IntegerType, StructField, StructType, TimestampType,StringType,DoubleType


log_dir_folder = prefix.lower()
input_dir = f"{bronze_table}"
log_dir = f"/user/hive/warehouse/log_dir_{log_dir_folder}"
data_changes_partition_key = ["DateKey"]
output_dir_primary_key = ["ROW_ID"]
output_dir_partition_key = "DateKey"
output_dir =f"{silver_table}"
data_changes_mode="merge"
silver_schema = StructType(
            [StructField("ROW_ID", StringType()),
             StructField("DATEKEY", IntegerType()), 
             StructField(f"{prefix}_TIMESTAMP", TimestampType()),
             StructField(f"{upper_metric_column}_SCORE", DoubleType()),
             StructField("METRIC",StringType()),
             StructField("FLG",StringType())]
        )

print(log_dir)
print(input_dir)
print(data_changes_partition_key)
#Create a Data Changes object with the input params
data_changes_input_dir = Changes(input_dir=input_dir, log_dir=log_dir,partition_key=data_changes_partition_key)
#call the determine function to get the changed partitions
partitions_input_dir = data_changes_input_dir.determine(mode=data_changes_mode, load_from=20230101)
print('partitions to load',[part[0] for part in partitions_input_dir])

    

df_silver = (spark.read.table(input_dir)
              .where(F.col('DATEKEY').isin([part[0] for part in partitions_input_dir]))
              .withColumn('FLG',F.when(F.col(f"{upper_metric_column}_SCORE") <= 5, f"Low {metric_column}").otherwise("Normal"))
              .where(F.col(f'{upper_metric_column}_SCORE').isNotNull())
              .where(F.col(f'{prefix}_TIMESTAMP').isNotNull())
              .select(["ROW_ID","DATEKEY",f"{prefix}_TIMESTAMP",f"{upper_metric_column}_SCORE","METRIC","FLG"]))


merge = Merge(
                    schema=silver_schema,
                    primary_key=["ROW_ID"],
                    partition_key="DATEKEY",
                    output_dir=f"{silver_table}"
                )

merge.execute(df_silver)
data_changes_input_dir.create_log_record()

