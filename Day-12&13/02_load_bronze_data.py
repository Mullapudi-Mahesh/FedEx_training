# Databricks notebook source
# MAGIC %md
# MAGIC Storage Access Setup

# COMMAND ----------

storage_account_name = "" 
access_key = "" 
spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", access_key)
print(f"Configuration for ADLS Storage ({storage_account_name}) has been successfully set.")

# COMMAND ----------

storage_account_name = "stdevsouthindianiiu"
container_name = "data"
access_key = ""
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    access_key
)

# COMMAND ----------

hotel_weather_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/hotel-weather/"

# COMMAND ----------

weather_df = spark.read.format("parquet").load(hotel_weather_path)
weather_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Generating Fernet key from .WHL file

# COMMAND ----------

from cryptography.fernet import Fernet
'''
key = Fernet.generate_key()
print(key.decode())  # Copy this string and store it securely
'''

# COMMAND ----------

# MAGIC %md
# MAGIC To create encryptor instance

# COMMAND ----------

from encryption_utils.data_encryptor import DataEncryptor  # import the class

encryption_key = b"KhDLOmf9CtoXV-AaWThxtgwgJHYkxVtle94uegfVdg8="

encryptor = DataEncryptor(encryption_key)  # instantiate the class


# COMMAND ----------

from pyspark.sql.functions import col

hotel_weather_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/hotel-weather/"

streaming_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")  .option("cloudFiles.schemaLocation", "dbfs:/schemas/hotel-weather/")  # Adjust if your files are JSON or CSV
    .load(hotel_weather_path)
)


# COMMAND ----------

pii_columns = ["address", "name"]
encrypted_df = encryptor.encrypt_columns(streaming_df, pii_columns)


# COMMAND ----------

display(encrypted_df)

# COMMAND ----------

checkpoint_path = "/mnt/delta/checkpoints/hotel_weather_raw"

(encrypted_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .outputMode("append")
    .table("bronze.hotel_weather_raw")  # specify database.table here
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hotel_weather_raw;

# COMMAND ----------

display(dbutils.fs.ls("/mnt/delta/checkpoints/hotel_weather_raw"))
