import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

from pyspark.sql.functions import current_timestamp
# Paths
bronze_path = "s3://learning-data-engineering-7799/ecommerce-data/bronze/bronze/products/products_dataset.csv"
silver_path = "s3://learning-data-engineering-7799/ecommerce-data/silver/products/"

# Read new data
df_new = spark.read.format("csv").option('inferSchema',True).option("header",True).load(bronze_path)
df_new = df_new.withColumn("ingestion_time", current_timestamp())


# Write output
df_new.write.mode("overwrite").parquet(silver_path)
job.commit()