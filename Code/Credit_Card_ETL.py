# Import modules 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, date_format, initcap, lower

# Inititlize spark session
spark = SparkSession.builder.appName("CreditCardETL").getOrCreate()

# Create dictionary for connection details for MySQL database
db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load CDW_SAPP_CUSTOMER.JSON data
customer_df = spark.read.json("cdw_sapp_customer.json")

# Apply transformation based on mapping document
customer_transformed_df = customer_df.select(
    # Direct move
    col("SSN").cast("int").alias("SSN"),
    # Convert first name to title case using initcap
    initcap(col("FIRST_NAME")).alias("FIRST_NAME"),
    # Convert middle name to lower case using lower
    lower(col("MIDDLE_NAME")).alias("MIDDLE_NAME"),
    # Convert last name to title case using initcap
    initcap(col("LAST_NAME")).alias("LAST_NAME"),
    # Direct move
    col("CREDIT_CARD_NO").alias("Credit_card_no"),
    # Concatenate apartment no and street name with comma as separator usinb concat_ws
    concat_ws(", ", col("STREET_NAME"), col("APT_NO")).alias("FULL_STREET_ADDRESS"),
    # Direct move
    col("CUST_CITY").alias("CUST_CITY"),
    # Direct move
    col("CUST_STATE").alias("CUST_STATE"),
    # Direct move
    col("CUST_COUNTRY").alias("CUST_COUNTRY"),
    # Direct move
    col("CUST_ZIP").cast("int").alias("CUST_ZIP"),
    # Change format of phone number to (###)###-####
    date_format(col("CUST_PHONE"), "(###)###-####").alias("CUST_PHONE"),
    # Direct Move
    col("CUST_EMAIL").alias("CUST_EMAIL"),
    # Direct Move
    col("LAST_UPDATED").cast("timestamp").alias("LAST_UPDATED")
)

# Write transformed CDW_SAPP_CUSTOMER data to target table in MySQL database using 
customer_transformed_df.write.jdbc(
    url=db_properties["url"],
    table="CDW_SAPP_CUSTOMER",
    mode="overwrite",
    properties=db_properties
)

# Load CDW_SAPP_BRANCH.JSON and apply transformations
cdw_sapp_branch_df = spark.read.json("CDW_SAPP_BRANCH.JSON")
branch_transformed_df = cdw_sapp_branch_df.select(
    col("BRANCH_CODE").cast("int").alias("BRANCH_CODE"),
    col("BRANCH_NAME").alias("BRANCH_NAME"),
    col("BRANCH_STREET").alias("BRANCH_STREET"),
    col("BRANCH_CITY").alias("BRANCH_CITY"),
    col("BRANCH_STATE").alias("BRANCH_STATE"),
    col("BRANCH_ZIP").cast("int").alias("BRANCH_ZIP"),
    date_format(col("BRANCH_PHONE"), "(###)###-####").alias("BRANCH_PHONE"),
    col("LAST_UPDATED").cast("timestamp").alias("LAST_UPDATED")
)

# Write CDW_SAPP_BRANCH data to target table
branch_transformed_df.write.jdbc(
    url=db_properties["url"],
    table="CDW_SAPP_BRANCH",
    mode="overwrite",
    properties=db_properties
)

# Load CDW_SAPP_CREDITCARD.JSON and apply transformations
cdw_sapp_credit_card_df = spark.read.json("CDW_SAPP_CREDITCARD.JSON")
credit_card_transformed_df = cdw_sapp_credit_card_df.select(
    col("CREDIT_CARD_NO").alias("CUST_CC_NO"),
    concat_ws("", col("YEAR"), col("MONTH"), col("DAY")).alias("TIMEID"),
    col("CUST_SSN").cast("int").alias("CUST_SSN"),
    col("BRANCH_CODE").cast("int").alias("BRANCH_CODE"),
    col("TRANSACTION_TYPE").alias("TRANSACTION_TYPE"),
    col("TRANSACTION_VALUE").cast("double").alias("TRANSACTION_VALUE"),
    col("TRANSACTION_ID").cast("int").alias("TRANSACTION_ID")
)

# Write CDW_SAPP_CREDIT_CARD data to target table
credit_card_transformed_df.write.jdbc(
    url=db_properties["url"],
    table="CDW_SAPP_CREDIT_CARD",
    mode="overwrite",
    properties=db_properties
)

# Stop the Spark session
#spark.stop()