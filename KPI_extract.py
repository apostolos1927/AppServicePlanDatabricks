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


# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

dbutils.widgets.dropdown("metric", "CpuPercentage",
                         ["CpuPercentage", "MemoryPercentage","DiskQueueLength","BytesReceived","BytesSent"], "metric")
metric= dbutils.widgets.get("metric")
print(metric)

dbutils.widgets.dropdown("metric_name", "CPU Percentage",
                         ["CPU Percentage", "Memory Percentage","Disk Queue Length","Data In","Data Out"], "metric_name")
metric_name= dbutils.widgets.get("metric_name")
print(metric_name)


dbutils.widgets.dropdown("prefix", "CPU",
                         ["CPU", "MEMORY","DISK","DATA_IN","DATA_OUT"], "prefix")
prefix= dbutils.widgets.get("prefix")
print(prefix)

dbutils.widgets.dropdown("table_suffix", "PERCENTAGE",
                         ["PERCENTAGE", "BYTES"], "table_suffix")
table_suffix= dbutils.widgets.get("table_suffix")
print(table_suffix)

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
# from my_test_code.__main__ import Merge, Changes
import os
from delta import DeltaTable
from datetime import datetime
from typing import Dict, List, Optional
from pyspark.sql import DataFrame, Row
from pyspark.sql.functions import col
from pyspark.sql.functions import max as spark_max


metric_url = "https://management.azure.com/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers/Microsoft.Web/serverfarms/{service_plan_app}/providers/Microsoft.Insights/metrics"
access_token_url = "https://login.microsoftonline.com/{tenant_id}/oauth2/token"
 
access_token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
 
access_token_data = {
    "grant_type": "client_credentials",
    "client_id": "....",
    "client_secret": ".....",
    "resource": "https://management.core.windows.net/"
}
try:
    response = requests.post(access_token_url, headers=access_token_headers, data=access_token_data)
except requests.exceptions.RequestException as e:
    print(e)
    raise Exception('Something went wrong')
print('response',response.text)


access_token = response.json()['access_token']
metric_params = {'metricnames': f'{metric}','api-version':'2018-01-01','timespan':f'{(date.today() - timedelta(days=5)).strftime("%Y-%m-%d")+"/"+date.today().strftime("%Y-%m-%d")}'}
# metric_params = {'metricnames': f'{metric}','api-version':'2018-01-01'}
metric_headers = {"Authorization": f"Bearer {access_token}"}
try:
   metric_response = requests.get(metric_url,params=metric_params, headers=metric_headers)
except requests.exceptions.RequestException as e:
   print(e)
   raise Exception('Something went wrong')


pretty_json = json.dumps(metric_response.json(), indent=4)
print(pretty_json)

df = pd.json_normalize(metric_response.json(),record_path=['value', 'timeseries','data'],meta=[['value','name']],errors='ignore')
df['metric'] = df['value.name'].apply(lambda x: x.get('localizedValue'))
df.drop(['value.name'], axis=1, inplace=True)
df = df.loc[df['metric'] == f'{metric_name}']
df = df.drop_duplicates(["timeStamp",f"{metric_column}","metric"])
df_bronze = spark.createDataFrame(df)


df_bronze = (df_bronze.withColumnRenamed(f"{metric_column}", f'{upper_metric_column}_SCORE')
     .withColumnRenamed('metric', 'METRIC')
     .withColumn(f'{prefix}_TIMESTAMP',F.to_timestamp(F.col("timeStamp")))
     .withColumn("DATEKEY",F.date_format(col(f'{prefix}_TIMESTAMP'), "yyyyMMdd").cast("integer"))
     .withColumn("ROW_ID", F.sha2(F.concat_ws("||",col("DATEKEY"),col(f"{prefix}_TIMESTAMP"),col(f'{upper_metric_column}_SCORE'),col("METRIC")), 256))
     .select(["ROW_ID","DATEKEY",f"{prefix}_TIMESTAMP",f"{upper_metric_column}_SCORE","METRIC"]))


source_schema = StructType(
            [StructField("ROW_ID", StringType()),StructField("DATEKEY", IntegerType()), StructField(f"{prefix}_TIMESTAMP", TimestampType()),StructField(f"{upper_metric_column}_SCORE", DoubleType()),StructField("METRIC",StringType())]
        )
merge = Merge(
                    schema=source_schema,
                    primary_key=["ROW_ID"],
                    partition_key="DATEKEY",
                    output_dir=f"BRONZE_{prefix}_{table_suffix}"
                )
print('output_dir',f"BRONZE_{prefix}_{table_suffix}")
merge.execute(df_bronze)
