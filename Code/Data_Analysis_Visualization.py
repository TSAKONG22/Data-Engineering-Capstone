from pyspark.sql import SparkSession
import matplotlib.pyplot as plt



# Create a Spark session
spark = SparkSession.builder.appName("DataAnalysisVisualization").getOrCreate()

# Create dictionary for connection details for MySQL database
db_properties = {
    "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load data from the database into DataFrames
transactions_df = spark.read.jdbc(db_properties["url"], "CDW_SAPP_CREDIT_CARD", properties=db_properties)
customers_df = spark.read.jdbc(db_properties["url"], "CDW_SAPP_CUSTOMER", properties=db_properties)

# Req-3.1: Plot transaction type with highest transaction count
transaction_count = transactions_df.groupBy("TRANSACTION_TYPE").count().orderBy("count", ascending=False).toPandas()
ax1 = transaction_count.plot(kind='bar', x='TRANSACTION_TYPE', y='count', title="Transaction Counts by Type", legend=False)
ax1.set_ylabel('Count')
ax1.set_xlabel('Transaction Type')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)
plt.tight_layout()
plt.show()

# Req-3.2: Find and plot which state has a high number of customers.
state_customer_count = customers_df.groupBy("CUST_STATE").count().orderBy("count", ascending=False).toPandas()
ax2 = state_customer_count.plot(kind='bar', x='CUST_STATE', y='count', title="Number of Customers by State", legend=False)
ax2.set_ylabel("Count")
ax2.set_xlabel("State")
plt.show()

# Req-3.3: Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
customer_transactions = transactions_df.groupBy("CUST_SSN").agg({'TRANSACTION_VALUE': 'sum'}).orderBy("sum(TRANSACTION_VALUE)", ascending=False).limit(10).toPandas()
ax3 = customer_transactions.plot(kind='bar', x='CUST_SSN', y='sum(TRANSACTION_VALUE)', title="Top 10 Customers by Transaction Amount", legend=False)
ax3.set_ylabel("Number of Transactions")
ax3.set_xlabel("Customer Number")
ax3.set_xticklabels(ax3.get_xticklabels(), rotation=45)
plt.tight_layout()
plt.show()

# Stop the Spark session
spark.stop()

