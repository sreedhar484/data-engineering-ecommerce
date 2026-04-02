import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from datetime import datetime, timedelta
from pyspark.sql.functions import col, to_date
import json
import boto3
from pyspark.sql.functions import date_sub,lit
from pyspark.sql import functions as sf

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


bronze_path = "s3://learning-data-engineering-7799/ecommerce-data/bronze/bronze/order_items/"
silver_path = "s3://learning-data-engineering-7799/ecommerce-data/silver/order_items/"
watermark_path = "s3://learning-data-engineering-7799/ecommerce-data/metadata/order_items_watermark.json"
s3 = boto3.client("s3")

def read_watermark():
    try:
        obj = s3.get_object(Bucket="learning-data-engineering-7799", Key="ecommerce-data/metadata/order_items_watermark.json")
        data = json.loads(obj["Body"].read())
        return data["last_processed_date"]
    except:
        return "1900-01-01"
last_processed = read_watermark()


df = spark.read.format("csv").option('inferSchema',True).option("header",True).load(bronze_path)
df = df.withColumn("shipping_limit_date", to_date(col("shipping_limit_date")))


df_incremental = df.filter(
    col("shipping_limit_date") >= date_sub(lit(last_processed), 2)
)
df_incremental = df_incremental.dropDuplicates(["order_id"])


df_incremental=df_incremental.withColumn("revenue",sf.col("price")+sf.col("freight_value"))

cutoff_date = (datetime.strptime(last_processed, "%Y-%m-%d") 
               - timedelta(days=2)).strftime("%Y-%m-%d")

replace_condition = f"shipping_limit_date >= '{cutoff_date}'"

df_incremental.write \
    .mode("overwrite") \
    .option("replaceWhere", replace_condition)\
    .partitionBy("shipping_limit_date") \
    .parquet(silver_path)
max_date = df_incremental.agg({"shipping_limit_date": "max"}).collect()[0][0]

s3.put_object(
    Bucket="learning-data-engineering-7799",
    Key="ecommerce-data/metadata/order_items_watermark.json",
    Body=json.dumps({"last_processed_date": str(max_date)})
)
job.commit()