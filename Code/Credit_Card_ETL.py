# Import modules 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws,initcap, lower, format_string, coalesce, lit
from pyspark.sql.types import StringType

# Inititlize spark session
spark = SparkSession.builder.appName("CreditCardETL").getOrCreate()

# Create dictionary for connection details for MySQL database
db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load customer json data
customer_df = spark.read.json(r"C:\Data-Engineering-Capstone\Data\cdw_sapp_custmer.json")

# Apply transformation based on mapping document
customer_transformed_df = customer_df.select(
    # Direct move
    col("SSN").cast("int").alias("SSN"),
    # Convert first name to title case using initcap
    initcap(col("FIRST_NAME")).cast(StringType()).alias("FIRST_NAME"),
    # Convert middle name to lower case using lower
    lower(col("MIDDLE_NAME")).cast(StringType()).alias("MIDDLE_NAME"),
    # Convert last name to title case using initcap
    initcap(col("LAST_NAME")).cast(StringType()).alias("LAST_NAME"),
    # Direct move
    col("CREDIT_CARD_NO").cast(StringType()).alias("Credit_card_no"),
    # Concatenate apartment no and street name with comma as separator using concat_ws
    concat_ws(", ", col("STREET_NAME"), col("APT_NO")).cast(StringType()).alias("FULL_STREET_ADDRESS"),
    # Direct move
    col("CUST_CITY").cast(StringType()).alias("CUST_CITY"),
    # Direct move
    col("CUST_STATE").cast(StringType()).alias("CUST_STATE"),
    # Direct move
    col("CUST_COUNTRY").cast(StringType()).alias("CUST_COUNTRY"),
    # Direct move
    col("CUST_ZIP").cast("int").alias("CUST_ZIP"),
    # Change format of phone number to (###)###-#### using format_string
    format_string("(%s)%s-%s",col("CUST_PHONE").substr(1,3), col("CUST_PHONE").substr(4,3), col("CUST_PHONE").substr(7,4)).cast(StringType()).alias("CUST_PHONE"),
    # Direct Move
    col("CUST_EMAIL").cast(StringType()).alias("CUST_EMAIL"),
    # Direct Move
    col("LAST_UPDATED").cast("timestamp").alias("LAST_UPDATED")
)

# Write transformed customer data to target table in MySQL database using 
customer_transformed_df.write.jdbc(
    url=db_properties["url"],
    table="CDW_SAPP_CUSTOMER",
    mode="overwrite",
    properties=db_properties
)

# Load branch json data and apply transformations
branch_df = spark.read.json(r"C:\Data-Engineering-Capstone\Data\cdw_sapp_branch.json")
branch_transformed_df = branch_df.select(
    # Direct move
    col("BRANCH_CODE").cast("int").alias("BRANCH_CODE"),
    # Direct move
    col("BRANCH_NAME").cast(StringType()).alias("BRANCH_NAME"),
    # Direct move
    col("BRANCH_STREET").cast(StringType()).alias("BRANCH_STREET"),
    # Direct move
    col("BRANCH_CITY").cast(StringType()).alias("BRANCH_CITY"),
    # Direct move
    col("BRANCH_STATE").cast(StringType()).alias("BRANCH_STATE"),
    # If source value is null, load dafault value (99999), value else direct move
    coalesce(col("BRANCH_ZIP").cast("int"), lit(99999)).alias("BRANCH_ZIP"),
    # Change format of phone number to (XXX)XXX-XXXX
    format_string("(%s)%s-%s", col("BRANCH_PHONE").substr(1,3), col("BRANCH_PHONE").substr(4,3), col("BRANCH_PHONE").substr(7,4)).cast(StringType()).alias("BRANCH_PHONE"),
    # Direct move
    col("LAST_UPDATED").cast("timestamp").alias("LAST_UPDATED")
)

# Write branch data to target table
branch_transformed_df.write.jdbc(
    url=db_properties["url"],
    table="CDW_SAPP_BRANCH",
    mode="overwrite",
    properties=db_properties
)
# Load credit card data and apply transformations
credit_card_df = spark.read.json(r"C:\Data-Engineering-Capstone\Data\cdw_sapp_credit.json")
credit_card_transformed_df = credit_card_df.select(
    col("CREDIT_CARD_NO").cast(StringType()).alias("CUST_CC_NO"),
    concat_ws("", col("YEAR"), col("MONTH"), col("DAY")).cast(StringType()).alias("TIMEID"),
    col("CUST_SSN").cast("int").alias("CUST_SSN"),
    col("BRANCH_CODE").cast("int").alias("BRANCH_CODE"),
    col("TRANSACTION_TYPE").cast(StringType()).alias("TRANSACTION_TYPE"),
    col("TRANSACTION_VALUE").cast("double").alias("TRANSACTION_VALUE"),
    col("TRANSACTION_ID").cast("int").alias("TRANSACTION_ID")
)

# Write credit card data to target table
credit_card_transformed_df.write.jdbc(
    url=db_properties["url"],
    table="CDW_SAPP_CREDIT_CARD",
    mode="overwrite",
    properties=db_properties
)

# Stop the Spark session
spark.stop()