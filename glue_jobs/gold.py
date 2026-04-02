import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date
import json
import boto3
from pyspark.sql.functions import date_sub,lit
from datetime import datetime, timedelta


sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

orders = spark.read.parquet("s3://learning-data-engineering-7799/ecommerce-data/silver/orders/")
order_items = spark.read.parquet("s3://learning-data-engineering-7799/ecommerce-data/silver/order_items/")
customers = spark.read.parquet("s3://learning-data-engineering-7799/ecommerce-data/silver/customers/")
products = spark.read.parquet("s3://learning-data-engineering-7799/ecommerce-data/silver/products/")

s3 = boto3.client("s3")

def read_watermark():
    try:
        obj = s3.get_object(Bucket="learning-data-engineering-7799", Key="ecommerce-data/metadata/fact_order_watermark.json")
        data = json.loads(obj["Body"].read())
        return data["last_processed_date"]
    except:
        return "1900-01-01"
last_processed = read_watermark()

# Reprocessing window (2 days back)


orders = orders.filter(
    col("order_purchase_date") >= date_sub(lit(last_processed), 2)
)
order_final=order_items.join(orders,on=order_items.order_id==orders.order_id,how="inner")\
    .join(products,on=order_items.product_id==products.product_id,how="inner")\
    .join(customers,on=orders.customer_id==customers.customer_id,how="inner")\
    .select(order_items.order_id,order_items.order_item_id,orders.customer_id,order_items.product_id,products.product_category_name,order_items.price,order_items.freight_value,order_items.revenue,orders.order_purchase_date,orders.order_delivered_customer_date,orders.order_status,orders.delivery_days)
order_final = order_final.dropDuplicates(["order_id", "order_item_id"])


cutoff_date = (datetime.strptime(last_processed, "%Y-%m-%d") 
               - timedelta(days=2)).strftime("%Y-%m-%d")

replace_condition = f"order_purchase_date >= '{cutoff_date}'"

order_final.write \
  .mode("overwrite") \
  .option("replaceWhere",replace_condition) \
  .partitionBy("order_purchase_date") \
  .parquet("s3://learning-data-engineering-7799/ecommerce-data/gold/fact/fact_orders/")

max_date = order_final.agg({"order_purchase_date": "max"}).collect()[0][0]

s3.put_object(
    Bucket="learning-data-engineering-7799",
    Key="ecommerce-data/metadata/fact_order_watermark.json",
    Body=json.dumps({"last_processed_date": str(max_date)})
)
job.commit()