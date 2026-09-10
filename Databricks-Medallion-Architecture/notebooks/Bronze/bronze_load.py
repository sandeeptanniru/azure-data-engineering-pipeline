#### data load into olist_customers_dataset table in bronze schema/database

from datetime import datetime

from pyspark.sql import functions as f

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    FloatType,
    TimestampType
)

#### Variables are declared below 
customer_table = 'retail_catalog.bronze.olist_customers_dataset'
geolocation_table = 'retail_catalog.bronze.olist_geolocation_dataset'
order_items_table = 'retail_catalog.bronze.olist_order_items_dataset'
order_reviews_table = 'retail_catalog.bronze.olist_order_reviews_dataset'
orders_table = 'retail_catalog.bronze.olist_orders_dataset'
products_table = 'retail_catalog.bronze.olist_products_dataset'
sellers_table = 'retail_catalog.bronze.olist_sellers_dataset'

print (f"Started Processing file - {customer_file} started at {datetime.now()}")
customer_schema = StructType([StructField("customer_id", StringType(), False),
                               StructField("customer_unique_id", StringType(), False),
                               StructField("customer_zip_code_prefix", IntegerType(), True),
                               StructField("customer_city", StringType(), True),
                               StructField("customer_state", StringType(), True)
])

customer_df1 = spark.read.format("csv").option("header","true") \
                                             .schema(customer_schema) \
                                             .option("mode","FAILFAST") \
                                             .load(customer_file)

customer_df = customer_df1.withColumn("Insert_Timestamp", f.current_timestamp())

##spark.sql("Truncate TABLE retail_catalog.raw.olist_customers_dataset")

print(f"Data Load into bronze table - {customer_table} started at {datetime.now()}")

customers_df.write.format("delta") \
                   .mode("overwrite") \
                   .saveAsTable(customer_table)

print(f"Data Load into bronze table - {customer_table} completed at {datetime.now()}")

print (f"Completed Processing file - {customer_file} completed  at {datetime.now()}")




#### data load into olist_geolocation_dataset table in bronze schema/database

print (f"Started Processing file - {geolocation_file} started at {datetime.now()}")

geolocation_schema = StructType([StructField("geolocation_zip_code_prefix", IntegerType(), True),
                                 StructField("geolocation_lat", FloatType(), True),
                                 StructField("geolocation_lng", FloatType(), True),
                                 StructField("geolocation_city", StringType(), True),
                                 StructField("geolocation_state", StringType(), True)
])

geolocation_df1 = spark.read.format("csv").option("header","true") \
                                          .schema(geolocation_schema) \
                                          .option("mode","FAILFAST") \
                                          .load(geolocation_file)

geolocation_df = geolocation_df1.withColumn("Insert_Timestamp", f.current_timestamp())

print(f"Data Load into bronze table - {geolocation_table} started at {datetime.now()}")

geolocation_df.write.format("delta").option("overwrite").saveAsTable(geolocation_table)

print(f"Data Load into bronze table - {geolocation_table} completed at {datetime.now()}")

print (f"Completed Processing file - {geolocation_file} completed at {datetime.now()}")




#### data load into olist_order_items_dataset table in bronze schema/database

print(f"started processing file - {order_items_file} started at {datetime.now()}")

order_items_schema = StructType([StructField("order_id", StringType(), False),
                                 StructField("order_item_id", IntegerType(), True),
                                 StructField("product_id", StringType(), True),
                                 StructField("seller_id", StringType(), True),
                                 StructField("shipping_limit_date", TimestampType(), True),
                                 StructField("price", FloatType(), True),
                                 StructType("freight_value", FloatType(), True)
                                 ])

order_items_df1 = spark.read.format("csv").option("header","true") \
                                          .schema(order_items_schema) \
                                          .option("mode", "FAILFAST") \
                                          .load(order_items_file)

order_items_df = order_items_df1.withColumn("Insert_Timestamp", f.current_timestamp())

print(f"Data Load into bronze table - {order_items_table} started at {datetime.now()}")

order_items_df.write.format("delta").option("mode", "overwrite").saveAsTable("order_items_table")

print(f"Data Load into bronze table - {order_items_table} completed at {datetime.now()}")

print (f"Completed Processing file - {order_items_file} completed at {datetime.now()}")




#### data load into olist_order_reviews_dataset table in bronze schema/database

print (f"Completed Processing file - {order_reviews_file} started at {datetime.now()}")

order_reviews_schema = StructType([StructField("review_id", StringType(), True),
                                   StructField("order_id", StringType(), True),
                                   StructField("review_score", IntegerType(), True),
                                   StructField("review_comment_title", StringType(), True),
                                   StructField("review_comment_message", StringType(), True),
                                   StructField("review_creation_date", TimestampType(), True),
                                   StructField("review_answer_timestamp", TimestampType, True)
                                   ])

order_reviews_df1 = spark.read.format("csv").option("header","true") \
                                            .schema(order_reviews_schema) \
                                            .option("mode", "FAILFAST") \
                                            .load(order_reviews_file)

order_reviews_df = order_reviews_df1.withColumn("Insert_Timestamp", f.current_timestamp())

print(f"Data Load into bronze table - {order_reviews_table} started at {datetime.now()}")

order_reviews_df.write.format("delta").option("overwrite").saveAsTable("order_reviews_table")

print(f"Data Load into bronze table - {order_reviews_table} completed at {datetime.now()}")

print (f"Completed Processing file - {order_reviews_file} completed at {datetime.now()}")



#### data load into olist_orders_dataset table in bronze schema/database

print (f"Started Processing file - {orders_file} started at {datetime.now()}")

orders_schema = StructType([StructField("order_id", StringType(), FALSE),
                            StructField("customer_id", StringType(), False),
                            StructField("order_status", StringType(), True),
                            StructField("order_purchase_timestamp", TimestampType(), True),
                            StructField("order_approved_at", TimestampType(), True),
                            StructField("order_delivered_carrier_date", TimestampType(), True),
                            StructField("order_delivered_customer_date", TimestampType(), True),
                            StructField("order_estimated_delivery_date", TimestampType(), True)
                            ])


orders_df1 = spark.read.format("csv").option("header","true") \
                                     .schema(orders_schema) \
                                     .option("mode", "FAILFAST") \
                                     .load(orders_file)

orders_df = orders_df1.withColumn("Insert_Timestamp", f.current_timestamp())

print(f"Data Load into bronze table - {orders_table} started at {datetime.now()}")

orders_df.write.format("delta").option("overwrite").saveAsTable("orders_table")

print(f"Data Load into bronze table - {orders_table} completed at {datetime.now()}")

print (f"Completed Processing file - {orders_file} completed at {datetime.now()}")


#### data load into olist_products_dataset table in bronze schema/database

print (f"Started Processing file - {products_file} started at {datetime.now()}")

products_schema = StructType([StructField("product_id", StringType(), True),
                              StructField("product_category_name", StringType(), True),
                              StructField("product_name_length", IntegerType(), True),
                              StructField("product_description_length", IntegerType(), True),
                              StructField("product_photos_qty", IntegerType(), True),
                              StructField("product_weight_g", IntegerType(), True),
                              StructField("product_length_cm", IntegerType(), True),
                              StructField("product_height_cm", IntegerType, True),
                              StructField("product_width_cm", IntegerType(), True)
                              ])


products_df1 = spark.read.format("csv").option("header","true") \
                                       .schema(products_schema) \
                                       .option("mode","FAILFAST") \
                                       .load("products_file")

products_df = products_df1.withColumn("Insert_Timestamp", f.current_timestamp())

print(f"Data Load into bronze table - {products_table} started at {datetime.now()}")

products_df.write.format("delta").option("overwrite").saveAsTable("products_table")

print(f"Data Load into bronze table - {products_table} completed at {datetime.now()}")

print (f"Started Processing file - {products_file} completed at {datetime.now()}")



#### data load into olist_sellers_dataset table in bronze schema/database

print (f"Started Processing file - {sellers_file} started at {datetime.now()}")

sellers_schema = StructType([StructField("seller_id", StringType(), False),
                             StructField("seller_zip_code_prefix", IntegerType(), False),
                             StructField("seller_city", StringType(), True),
                             StructField("seller_state", StringType(), True)
])

sellers_df1 = spark.read.format("csv").option("header","true") \
                                      .schema(sellers_schema) \
                                      .option("mode", "FAILFAST") \
                                      .load("sellers_file")

sellers_df = sellers_df1.withcolumn("Insert_Timestamp", f.current_timestamp())

print(f"Data Load into bronze table - {sellers_table} started at {datetime.now()}")

sellers_df.write.format("delta").option("overwrite").saveAsTable("sellers_table")

print(f"Data Load into bronze table - {sellers_table} completed at {datetime.now()}")

print (f"Started Processing file - {sellers_file} completed at {datetime.now()}")
