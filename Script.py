import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import (
    col, trim, split, regexp_extract,
    to_timestamp, month, hour
)

# --------------------------
# Job Initialization
# --------------------------
args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "INPUT_PATH", "OUTPUT_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

input_path = args["INPUT_PATH"]
output_path = args["OUTPUT_PATH"]

# --------------------------
# 1. Extract
# --------------------------
raw_df = spark.read.option("header", True).csv(input_path)

print("Raw row count:", raw_df.count())

# --------------------------
# 2. Transform
# --------------------------

df = (
    raw_df
    .withColumn("Order ID", trim(col("Order ID")))
    .withColumn("Product", trim(col("Product")))
    .withColumn("Quantity Ordered", col("Quantity Ordered").cast("int"))
    .withColumn("Price Each", col("Price Each").cast("double"))
)

# Remove empty (not null) rows safely
df = df.filter(
    (col("Order ID") != "") &
    (col("Product") != "")
)

# Convert date
df = df.withColumn(
    "Order_Timestamp",
    to_timestamp(col("Order Date"), "MM/dd/yy HH:mm")
)

# Enrich
df = (
    df
    .withColumn("Order Month", month(col("Order_Timestamp")))
    .withColumn("Order Hour", hour(col("Order_Timestamp")))
    .withColumn("Sales", col("Quantity Ordered") * col("Price Each"))
)

# Address parsing
df = (
    df
    .withColumn("City", trim(split(col("Purchase Address"), ",").getItem(1)))
    .withColumn("State", regexp_extract(col("Purchase Address"), r",\s([A-Z]{2})\s", 1))
    .withColumn("Zipcode", regexp_extract(col("Purchase Address"), r"(\d{5})$", 1))
)

print("Transformed row count:", df.count())

df.show(5, truncate=False)

# --------------------------
# 3. Load (WRITE ONCE)
# --------------------------
dyf = DynamicFrame.fromDF(df, glueContext, "sales_dyf")

glueContext.write_dynamic_frame.from_options(
    frame=dyf,
    connection_type="s3",
    connection_options={"path": output_path},
    format="parquet"
)

job.commit()
