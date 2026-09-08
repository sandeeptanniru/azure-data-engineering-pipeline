#### Variables used in the notebook are declared here

file_path = '/Volumes/retail_catalog/raw/source_files'

files = dbutils.fs.ls(file_path)

customer_file = next(
    (f.path for f in files
    if f.name.startswith("olist_customers_dataset") and f.name.endswith(".csv")
    ), 
    None
)

if customer_file:
    print("Expected customer file found:", customer_file)
else:
    print("Expected customer file not found")


geolocation_file = next(
    (f.path for f in files
    if f.name.startswith("olist_geolocation_dataset") and f.name.endswith(".csv")),
    None
)

if geolocation_file:
    print("Expected geolocation file found:", geolocation_file)
else:
    print("Expected geolocation file not found")



order_items_file = next(
    (f.path for f in files
    if f.name.startswith("olist_order_items") and f.name.endswith(".csv")),
    None
)

if order_items_file:
    print("Expected order items file found:", order_items_file)
else:
    print("Expected order items file not found")



order_reviews_file = next(
    (f.path for f in files
    if f.name.startswith("olist_order_reviews") and f.name.endswith(".csv")),
    None
)

if order_reviews_file:
    print("Expected order reviews file found:", order_reviews_file)
else:
    print("Expected order review file not found")


orders_file = next(
    (f.path for f in files
    if f.name.startswith("olist_orders") and f.name.endswith(".csv")),
    None
)

if orders_file:
    print("Expected orders_file found:", orders_file)
else:
    print("Expected orders file not found")

products_file = next(
    (f.path for f in files
    if f.name.startswith("olist_products") and f.name.endswith(".csv")),
    None
)

if products_file:
    print("Expected products file found:", products_file)
else:
    print("Expected products file not found")

sellers_file = next(
    (f.path for f in files
    if f.name.startswith("olist_sellers") and f.name.endswith(".csv")),
    None
)

if sellers_file:
    print("Expected sellers file found:", sellers_file)
else:
    print("Expected sellers file not found")
