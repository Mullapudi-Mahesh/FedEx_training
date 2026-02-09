# Databricks notebook source
silver_table_path = "/mnt/delta/hotel_weather_processed"  # or the correct path/name of your table

silver_stream_df = (
    spark.readStream
    .format("delta")
    .table("hotel_weather_processed")  # or use .load(silver_table_path)
)


# COMMAND ----------

from pyspark.sql.functions import col
from encryption_utils.data_encryptor import DataEncryptor
# Use the same encryption key you used before
encryption_key = b"KhDLOmf9CtoXV-AaWThxtgwgJHYkxVtle94uegfVdg8="  # example key, replace with yours

# Create encryptor instance
encryptor = DataEncryptor(encryption_key)


# COMMAND ----------

# MAGIC %md
# MAGIC Decrypt PII columns

# COMMAND ----------

# List your PII columns that were encrypted, for example:
pii_columns = ["name", "address"]

# Decrypt the PII columns using your encryptor instance
decrypted_df = encryptor.decrypt_columns(silver_stream_df, pii_columns)


# COMMAND ----------

# MAGIC %md
# MAGIC Aggregated metrics

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct, avg, max, min, expr

agg_df = (
    decrypted_df
    .groupBy("country", "city", "wthr_date")
    .agg(
        approx_count_distinct("id").alias("num_distinct_hotels"),
        avg("avg_tmpr_c").alias("avg_temperature_c"),
        max("avg_tmpr_c").alias("max_temperature_c"),
        min("avg_tmpr_c").alias("min_temperature_c")
    )
)

agg_df = agg_df.withColumn("temperature_difference_c", expr("max_temperature_c - min_temperature_c"))


# COMMAND ----------

display(agg_df)

# COMMAND ----------

# MAGIC %md
# MAGIC streaming aggregation result to Gold Delta Table

# COMMAND ----------

query = (
    agg_df.writeStream
    .format("delta")
    .outputMode("complete")
    .option("checkpointLocation", "/path/to/gold/checkpoint/hotel_weather_metrics")
    .table("gold.hotel_weather_metrics")  # This starts the stream internally
)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hotel_weather_metrics;

# COMMAND ----------

# MAGIC %md
# MAGIC **6.Visualize the Results with a Databricks Dashboard**

# COMMAND ----------

# MAGIC %md
# MAGIC Identify Top 5 Cities

# COMMAND ----------

# MAGIC %sql DESCRIBE gold.hotel_weather_metrics
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql import functions as F

# Read gold table in batch mode
gold_df = spark.table("hotel_weather_metrics")

# Get Top 5 cities by total number of reported hotels
top_cities = (
    gold_df.groupBy("country", "city")
    .agg(F.max("num_distinct_hotels").alias("max_hotels"))
    .orderBy(F.col("max_hotels").desc())
    .limit(5)
    .collect()
)

top_cities_list = [(row['country'], row['city']) for row in top_cities]


# COMMAND ----------

display(top_cities_list)

# COMMAND ----------

for i, (country, city) in enumerate(top_cities_list):
    city_view_name = f"top_city_{i+1}_view"
    (
        gold_df
        .filter((col("city") == city) & (col("country") == country))
        .select(
            "wthr_date",
            col("num_distinct_hotels").alias("number_of_reported_hotels"),
            col("avg_temperature_c").alias("avg_tmpr_c"),
            col("max_temperature_c").alias("max_tmpr_c"),
            col("min_temperature_c").alias("min_tmpr_c"),
        )
        .createOrReplaceTempView(city_view_name)
    )
    print(f"Created view: {city_view_name} for {city}, {country}")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: top_city_1_view
# MAGIC SELECT * FROM top_city_1_view ORDER BY wthr_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: top_city_1_view
# MAGIC SELECT * FROM top_city_2_view ORDER BY wthr_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: top_city_1_view
# MAGIC SELECT * FROM top_city_3_view ORDER BY wthr_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: top_city_1_view
# MAGIC SELECT * FROM top_city_4_view ORDER BY wthr_date
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View: top_city_1_view
# MAGIC SELECT * FROM top_city_5_view ORDER BY wthr_date
# MAGIC