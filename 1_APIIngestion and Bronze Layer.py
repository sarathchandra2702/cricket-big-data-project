# Databricks notebook source
# DBTITLE 1,Import the Required Library
import requests
import json
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Create Catalog, Schema, and Volume
spark.sql('CREATE CATALOG IF NOT EXISTS workspace')
spark.sql('CREATE SCHEMA IF NOT EXISTS workspace.default')
spark.sql('CREATE VOLUME IF NOT EXISTS workspace.default.cricket_api_project')

base_path = '/Volumes/workspace/default/cricket_api_project'

# COMMAND ----------

# DBTITLE 1,CALLING Cricket API
API_KEY = '32b92c2a-93db-4e08-93d9-dd820e126931'
api_url = f"https://api.cricapi.com/v1/currentMatches?apikey={API_KEY}&offset=0"

response = requests.get(api_url)
response.raise_for_status()

api_data = response.json()
print(api_data.keys())

print(json.dumps(api_data, indent=2)[:2000])

# COMMAND ----------

raw_file_path = f'{base_path}/current_matches_raw.json'

with open(raw_file_path, 'w') as file:
  json.dump(api_data,file)

print("RAW API data is save at the :",raw_file_path)

# COMMAND ----------

# DBTITLE 1,CREATE BRONZE LAYER DATAFRAME OR TABLE
bronze_data = [{
    "source_api":api_url,
    "raw_json":json.dumps(api_data),
    "ingestion_time":None
}]

bronze_schema = StructType([
    StructField("source_api", StringType(), True),
    StructField("raw_json", StringType(), True),
    StructField("ingestion_time", TimestampType(), True)
])

bronze_df=spark.createDataFrame(bronze_data, schema=bronze_schema).withColumn("ingestion_time", current_timestamp())

display(bronze_df)

# COMMAND ----------

# DBTITLE 1,Save the Bronze Table
bronze_df.write\
    .format('delta')\
    .mode('overwrite')\
    .saveAsTable('workspace.default.cricket_bronze_current_matches')

print("BRONZE TABLE CREATED SUCCESSFULLY")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.default.cricket_bronze_current_matches

# COMMAND ----------

