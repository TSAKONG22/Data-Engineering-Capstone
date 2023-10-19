# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, when, month, year, substring, cast
import matplotlib.pyplot as plt

# Initialize Spark session
spark = SparkSession.builder.appName("Loan Analysis").getOrCreate()

# Create dictionary for connection details for MySQL database
db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# load the loan application data
loan_df = spark.read.jdbc(db_properties["url"], "CDW_SAPP_loan_application", properties=db_properties)
# load the branch data
branch_df = spark.read.jdbc(db_properties["url"], "CDW_SAPP_BRANCH", properties=db_properties)
# load the credit card data
credit_card_df = spark.read.jdbc(db_properties["url"], table="CDW_SAPP_CREDIT_CARD", properties=db_properties)

# Req-5.1: Find and plot the percentage of applications approved for self-employed applicants.
self_employed_approved_count = loan_df.filter((col("Self_Employed") == "Yes") & (col("Application_Status") == "Y")).count()
self_employed_total_count = loan_df.filter(col("Self_Employed") == "Yes").count()
self_employed_approved_percentage = (self_employed_approved_count / self_employed_total_count) * 100

plt.bar(['Self Employed Approved'], [self_employed_approved_percentage])
plt.ylim(0,100)
plt.ylabel('Percentage (%)')
plt.title('Loan Approval Percentage by Employment Type')
plt.show()

# Req-5.2: Find the percentage of rejection for married male applicants.
married_male_rejected_count = loan_df.filter((col("Gender") == "Male") & (col("Married") == "Yes") & (col("Application_Status") == "N")).count()
married_male_total_count = loan_df.filter((col("Gender") == "Male") & (col("Married") == "Yes")).count()
married_male_rejected_percentage = (married_male_rejected_count / married_male_total_count) * 100
print(f"Percentage of Rejection for Married Male Applicants: {married_male_rejected_percentage:.2f}%")

# Req-5.3: Find and plot the top three months with the largest volume of transaction data, assuming its for credit card transactions data
transaction_counts = (credit_card_df
    .withColumn("Month", substring(credit_card_df["TIMEID"], 5, 2).cast("int"))
    .groupBy("Month")
    .count()
    .orderBy(col("count").desc())
    .limit(3))
transaction_counts.show()
ax1 = transaction_counts.toPandas().plot(x='Month', y='count', kind='bar', title='Top 3 Months with Largest Volume of Transactions', legend=False)
plt.ylabel('Transaction Count')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
plt.show()

# Req-5.4: Plot brach that processed highest total dollar value of healthcare transactions, assuming the data is from credit card data
healthcare_transactions = credit_card_df.filter(col("TRANSACTION_TYPE") == "Healthcare").groupBy("BRANCH_CODE").agg(sum("TRANSACTION_VALUE").alias("Total Amount")).orderBy(col("Total Amount").desc()).limit(1)
healthcare_transactions.show()
ax2 = healthcare_transactions.toPandas().plot(x='BRANCH_CODE', y='Total Amount', kind='bar', title='Branch with Highest $ Value of Healthcare Transactions', legend=False)
plt.ylabel('Total Dollar Value')
ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45)
plt.show()

# Stop spark sesssion
spark.stop()