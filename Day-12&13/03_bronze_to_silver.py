# Databricks notebook source
# MAGIC %md
# MAGIC Read bronze table using readStream

# COMMAND ----------

bronze_df = (
    spark.readStream
    .format("delta")
    .table("hotel_weather_raw")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Decrypt PII columns

# COMMAND ----------

from encryption_utils.data_encryptor import DataEncryptor  
encryption_key = b"KhDLOmf9CtoXV-AaWThxtgwgJHYkxVtle94uegfVdg8=" 
encryptor = DataEncryptor(encryption_key)

# COMMAND ----------

# Reuse the same encryption key and instance you created earlier
decrypted_df = encryptor.decrypt_columns(bronze_df, ["address", "name"])


# COMMAND ----------

# MAGIC %md
# MAGIC Trim whitespace

# COMMAND ----------

from pyspark.sql.functions import trim, col

trimmed_df = (
    decrypted_df
    .withColumn("address", trim(col("address")))
    .withColumn("city", trim(col("city")))
    .withColumn("country", trim(col("country")))
    .withColumn("geoHash", trim(col("geoHash")))
    .withColumn("id", trim(col("id")))
    .withColumn("name", trim(col("name")))
    .withColumn("wthr_date", trim(col("wthr_date")))
    .withColumn("_rescued_data", trim(col("_rescued_data")))
)


# COMMAND ----------

# MAGIC %md
# MAGIC Standardize date format

# COMMAND ----------

from pyspark.sql.functions import to_date

date_standardized_df = trimmed_df.withColumn(
    "wthr_date", to_date(col("wthr_date"), "yyyy-MM-dd")
)


# COMMAND ----------

# MAGIC %md
# MAGIC Cast data types for temperature fields

# COMMAND ----------

from pyspark.sql.functions import col

casted_df = (
    date_standardized_df
    .withColumn("avg_tmpr_c", col("avg_tmpr_c").cast("double"))
    .withColumn("avg_tmpr_f", col("avg_tmpr_f").cast("double"))
)


# COMMAND ----------

# MAGIC %md
# MAGIC Replacing blanks strings with Nulls

# COMMAND ----------

from pyspark.sql.functions import when

null_replaced_df = (
    casted_df
    .withColumn("name", when(col("name") == "", None).otherwise(col("name")))
    .withColumn("address", when(col("address") == "", None).otherwise(col("address")))
)


# COMMAND ----------

# MAGIC %md
# MAGIC Drop rows with nulls 

# COMMAND ----------

cleaned_df = null_replaced_df.dropna(subset=["id", "wthr_date", "name", "address","city","country"])


# COMMAND ----------

# MAGIC %md
# MAGIC Remove duplicates based on id and wthr_date

# COMMAND ----------

deduplicated_df = cleaned_df.dropDuplicates(["id", "wthr_date"])


# COMMAND ----------

# MAGIC %md
# MAGIC Encrypt PII columns in Streaming data 

# COMMAND ----------

# List PII columns to encrypt
pii_columns = ["address", "name"]

# Encrypt PII fields
encrypted_df = encryptor.encrypt_columns(deduplicated_df, pii_columns)


# COMMAND ----------

checkpoint_path = "/mnt/delta/checkpoints/hotel_weather_processed"  # Change to your storage location

query = (
    encrypted_df.writeStream
    .format("delta")
    .outputMode("append")  # Use append for streaming writes
    .option("checkpointLocation", checkpoint_path)
    .table("silver.hotel_weather_processed")  # Use silver database prefix here
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hotel_weather_processed;