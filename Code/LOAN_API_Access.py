# Import libraries
from pyspark.sql import SparkSession
import requests
from pyspark.sql import DataFrame

# Initialize spark session
spark = SparkSession.builder.appName("LoanApplicationAPI").getOrCreate()

# Req 4.1: get data from endpoint
response = requests.get("https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json")

# Req 4.2: print status code of api response
print(f"Status code of the API response: {response.status_code}")

if response.status_code == 200:
    # Convert the JSON response to pyspark dataframe
    data = response.json()
    loan_df: DataFrame = spark.createDataFrame(data)

    # Req 4.3:

    # Create dictionary for connection details for MySQL database
    db_properties = {
        "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
        "user": "root",
        "password": "password",
        "driver": "com.mysql.cj.jdbc.Driver"
    }
    # Load data in MySQL database
    loan_df.write.jdbc(url=db_properties["url"], table="CDW_SAPP_loan_application", mode="overwrite", properties=db_properties)

    print("Data successfully loaded into RDBMS.")

else:
    print(f"Failed to fetch data. Status code: {response.status_code}")

# Stop the Spark session
spark.stop()