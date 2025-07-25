import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read data from Glue catalog
datasource = glueContext.create_dynamic_frame.from_catalog(
    database="data_eng-sales-data",
    table_name="raw"
)

# Transformations
# 1. Remove ₹ from prices and convert to float
# 2. Remove commas from rating_count and convert to integer
# 3. Split category into array
transformed = datasource.apply_mapping([
    ("product_id", "string", "product_id", "string"),
    ("product_name", "string", "product_name", "string"),
    ("category", "string", "category", "string"),
    ("discounted_price", "string", "discounted_price", "float"),
    ("actual_price", "string", "actual_price", "float"),
    ("discount_percentage", "string", "discount_percentage", "float"),
    ("rating", "string", "rating", "float"),
    ("rating_count", "string", "rating_count", "int"),
    ("about_product", "string", "about_product", "string"),
    ("user_id", "string", "user_id", "string"),
    ("user_name", "string", "user_name", "string"),
    ("review_id", "string", "review_id", "string"),
    ("review_title", "string", "review_title", "string"),
    ("review_content", "string", "review_content", "string"),
    ("img_link", "string", "img_link", "string"),
    ("product_link", "string", "product_link", "string")
])

# Custom transformation using Spark DataFrame
df = transformed.toDF()
from pyspark.sql.functions import col, regexp_replace, split
df_transformed = df.withColumn("discounted_price", regexp_replace(col("discounted_price"), "[₹,]", "").cast("float")) \
                   .withColumn("actual_price", regexp_replace(col("actual_price"), "[₹,]", "").cast("float")) \
                   .withColumn("discount_percentage", regexp_replace(col("discount_percentage"), "%", "").cast("float")) \
                   .withColumn("rating_count", regexp_replace(col("rating_count"), ",", "").cast("int")) \
                   .withColumn("category_array", split(col("category"), "\\|"))

# Convert back to DynamicFrame
transformed_frame = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_frame")

# Write to S3 in Parquet format
glueContext.write_dynamic_frame.from_options(
    frame=transformed_frame,
    connection_type="s3",
    connection_options={"path": "s3://sales-data-dataengineeringproject-us-east1/transformed/"},
    format="parquet"
)

job.commit()
