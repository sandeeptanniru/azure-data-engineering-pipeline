
#### data load into dataframe
staging_table = 'retail_catalog.bronze.olist_customers_dataset'

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)
customers_schema = StructType([StructField("customer_id", StringType(), False),
                               StructField("customer_unique_id", StringType(), False),
                               StructField("customer_zip_code_prefix", IntegerType(), True),
                               StructField("customer_city", StringType(), True),
                               StructField("customer_state", StringType(), True)
])

customers_df = spark.read.format("csv").option("header","true") \
                                             .schema(customers_schema) \
                                             .load(customer_file)
                    
customers_df.show()

##spark.sql("Truncate TABLE retail_catalog.raw.olist_customers_dataset")

customers_df.write.format("delta") \
                   .mode("append") \
                   .saveAsTable(staging_table)

spark.sql("SELECT COUNT(*) FROM retail_catalog.raw.olist_customers_dataset")