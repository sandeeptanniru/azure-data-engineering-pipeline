
###

from pyspark.sql import functions


%run ../Setup/common_config.py

def remove_duplicates(df):
    return(df.dropDuplicates())

print (f"Started sliver load at {datetime.now()}")

print(f"Data Load into silver table - {customer_silver_table} started at {datetime.now()}")

customer_df1 = spark.read.table(f"{catalog_name}.{bronze_schema}.{customer_table}")

customer_df = customer_df1.select(col("Batch_ID"), 
                                  col("customer_unique_id").alias("unique_id"), 
                                  col("customer_zip_code_prefix").alias("zip_code"),
                                  col("customer_city").alias("city"),
                                  col("customer_state").alias("state"),
                                  col("load_timestamp")) \
                        .filter(col("customer_unique_id").isNotNull)


customer_df = remove_duplicates(customer_df)

customer_df.show()

customer_df.write.format("delta").mode("overwrite") \
                                 .saveAsTable(f"{catalog_name}.{silver_schema}.{customer_silver_table}")

print(f"Data Load into silver table - {customer_silver_table} completed at {datetime.now()}")


print(f"Data Load into silver table - {geolocation_silver_table} started at {datetime.now()}")

geolocation_df1 = spark.read.table(f"{catalog_name}.{bronze_schema}.{geolocation_table}")

geolocation_df = geolocation_df1.select(col("Batch_ID"),
                                        col("geolocation_zip_code_prefix").alias("zip_code"),
                                        col("geolocation_lat").alias("latitude"),
                                        col("geolocation_lng").alias("longitude"),
                                        col("geolocation_city").alias("city"),
                                        col("geolocation_state").alias("state")
                                        col("load_timestamp")) \
                                .filter(col("geolocation_zip_code_prefix").isNotNull)

geolocation_df = remove_duplicates(geolocation_df)

geolocation_df.show()

geolocation_df.write.format("delta").mode("overwrite") \
                                    .saveAsTable(f"{catalog_name}.{silver_schema}.{geolocation_silver_table}")

print(f"Data Load into silver table - {geolocation_silver_table} completed at {datetime.now()}")


print(f"Data Load into silver table - {order_items_silver_table} started at {datetime.now()}")

order_items_df1 = spark.read.table(f"{catalog_name}.{bronze_schema}.{order_items_table}")

order_items_df = order_items_df1.withColumnRenamed("freight_value", "shipping_cost") \
                                .withColumn("total_cost", (col("shipping_cost")+col("price")))


order_items_df = remove_duplicates(order_items_df)

order_items_df.show()

order_items_df.write.format("delta").mode("overwrite") \
                                    .saveAsTable(f"{catalog_name}.{silver_schema}.{order_items_silver_table}")

print(f"Data Load into silver table - {order_items_silver_table} completed at {datetime.now()}")



print(f"Data Load into silver table - {order_reviews_silver_table} started at {datetime.now()}")

order_reviews_df2 = spark.read.table(f"{catalog_name}.{bronze_schema}.{order_reviews_table}")

order_reviews_df = order_reviews_df2.withColumnRenamed("review_score", "rating") \
                                    .withColumnRenmaed("review_comment_title","review_title") \
                                    .withColumnRenmaed("review_comment_message", "review_comment")\
                                    .withColumnRenamed("review_answer_timestamp", "review_answer_date")

order_reviews_df = remove_duplicates(order_reviews_df)

order_reviews_df.show()

order_reviews_df.write.format("delta").mode("overwrite") \
                                    .saveAsTable(f"{catalog_name}.{silver_schema}.{order_reviews_silver_table}")

print(f"Data Load into silver table - {order_reviews_silver_table} completed at {datetime.now()}")


print(f"Data Load into silver table - {orders_silver_table} started at {datetime.now()}")

orders_df2 = spark.read.table(f"{catalog_name}.{bronze_schema}.{orders_table}")

orders_df = orders_df2.withColumnRenamed("order_purchase_timestamp", "order_date") \
                      .withCoulmnRenamed("order_approved_at", "payment_date") \
                      .withColumnRenamed("order_delivered_carrier_date", "shipping_date") \
                      .withColumnRenamed("order_delivered_customer_date", "delivery_date") \
                      .withColumnRenamed("order_estimated_delivery_date", "estimated_delivery_date")

orders_df = remove_duplicates(orders_df)

orders_df.show()

orders_df.write.format("delta").mode("overwrite") \
                                    .saveAsTable(f"{catalog_name}.{silver_schema}.{orders_silver_table}")

print(f"Data Load into silver table - {orders_silver_table} completed at {datetime.now()}")


print(f"Data Load into silver table - {products_silver_table} started at {datetime.now()}")

products_df2 = spark.read.table(f"{catalog_name}.{bronze_schema}.{products_table}")

products_df = products_df2.withColumnRenamed("product_name_lenght", "product_name_length") \
                          .withCoulmnRenamed("product_description_lenght", "product_description_length") \
                          .withColumnRenamed("product_photos_qty", "product_photo_count") \
                          .withColumnRenamed("product_weight_g", "product_weight_grams") \
                          .withColumnRenamed("product_length_cm", "product_length_cm") \
                          .withColumnRenamed("product_height_cm", "product_height_cm") \
                          .withColumnRenamed("product_width_cm", "product_width_cm")

products_df = remove_duplicates(products_df)

products_df.show()

products_df.write.format("delta").mode("overwrite") \
                                    .saveAsTable(f"{catalog_name}.{silver_schema}.{products_silver_table}")

print(f"Data Load into silver table - {products_silver_table} completed at {datetime.now()}")


print(f"Data Load into silver table - {sellers_silver_table} started at {datetime.now()}")

sellers_df2 = spark.read.table(f"{catalog_name}.{bronze_schema}.{sellers_table}")

sellers_df = sellers_df2.withColumnRenamed("seller_zip_code_prefix", "sellers_zip_code")

sellers_df = remove_duplicates(sellers_df)

sellers_df.show()

sellers_df.write.format("delta").mode("overwrite") \
                                    .saveAsTable(f"{catalog_name}.{silver_schema}.{sellers_silver_table}")

print(f"Data Load into silver table - {sellers_silver_table} completed at {datetime.now()}")

print (f"Completed sliver load at {datetime.now()}")
