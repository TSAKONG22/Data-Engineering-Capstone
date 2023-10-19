from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Transaction_Customer_App").getOrCreate()


# Create dictionary with connection details for MySQL database
db_properties = {
     "url": "jdbc:mysql://localhost:3306/creditcard_capstone",
    "user": "root",
    "password": "password",  
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Load dataframes from MySQL
credit_card_df = spark.read.jdbc(url=db_properties["url"], table="CDW_SAPP_CREDIT_CARD", properties=db_properties)
customer_df = spark.read.jdbc(url=db_properties["url"], table="CDW_SAPP_CUSTOMER", properties=db_properties)

# Req-2.1

# Filter and display transaction by Zipcode, month and year
def transaction_by_zip_month_year():
    zip_code = input("Enter the zip code: ")
    month = input("Enter the month (MM): ")
    year = input("Enter the year (YYYY): ")

    result = credit_card_df.filter((credit_card_df.CUST_ZIP == zip_code) & 
                                   (credit_card_df.TIMEID.startswith(year + month)))
    result.show()

# Display the total number and value of transactions for a given type
def transaction_by_type():
    transaction_type = input("Enter the transaction type: ")
    result = credit_card_df.filter(credit_card_df.TRANSACTION_TYPE == transaction_type)
    print(f"Total Transactions: {result.count()}")
    print(f"Total Value: {result.groupBy().sum('TRANSACTION_VALUE').collect()[0][0]}")

# Display the total number and value of transaction for branches in a specific state
def transaction_by_branch_state():
    state = input("Enter the branch state: ")
    result = credit_card_df.filter(credit_card_df.BRANCH_STATE == state)
    print(f"Total Transactions: {result.count()}")
    print(f"Total Value: {result.groupBy().sum('TRANSACTION_VALUE').collect()[0][0]}")

# Req-2.2 Functions

# Display the details of a customer based on their SSN. Assuming, SSN are unique identifier for every individual
def check_existing_account():
    ssn = input("Enter the SSN of the customer: ")
    result = customer_df.filter(customer_df.SSN == ssn)
    result.show()

# Modify a chosen field for a specific customer based on their SSn
def modify_account_details():
    ssn = input("Enter the SSN of the customer to modify: ")
    result = customer_df.filter(customer_df.SSN == ssn)

    # Check if user exists
    if not result.head():
        print("No such customer.")
        return

    print("Current details of the customer:")
    result.show()

    # Getting fields to modify
    print("\nWhich field would you like to modify?")
    print("1. SSN")
    print("2. FIRST_NAME")
    print("3. MIDDLE_NAME")
    print("4. LAST_NAME")
    print("5. CREDIT_CARD_NO")
    print("6. STREET_NAME, APT_NO")
    print("7. CUST_CITY")
    print("8. CUST_STATE")
    print("9. CUST_COUNTRY")
    print("10. CUST_ZIP")
    print("11. CUST_PHONE")
    print("12. CUST_EMAIL")
    print("13. LAST_UPDATED")
    choice = input("Enter the number of your choice (e.g., '1' for SSN): ")

    column_mapping = {
        '1': 'SSN',
        '2': 'FIRST_NAME',
        '3': 'MIDDLE_NAME',
        '4': 'LAST_NAME',
        '5': 'CREDIT_CARD_NO',
        '6': 'STREET_NAME',
        '7': 'CUST_CITY',
        '8': 'CUST_STATE',
        '9': 'CUST_COUNTRY',
        '10': 'CUST_ZIP',
        '11': 'CUST_PHONE',
        '12': 'CUST_EMAIL',
        '13': 'LAST_UPDATED'
    }

    if choice in column_mapping:
        column_name = column_mapping[choice]
        new_value = input(f"Enter the new value for {column_name}: ")

        # Update the DataFrame
        updated_df = customer_df.withColumn(column_name, when(col('SSN') == ssn, new_value).otherwise(col(column_name)))

        # Writing changes back to MySQL
        updated_df.write.jdbc(url=db_properties["url"], table="CDW_SAPP_CUSTOMER", mode="overwrite", properties=db_properties)
        print(f"{column_name} updated successfully.")
    else:
        print("Invalid choice. Returning to main menu.")

# Generate the montly bill for a credit card for a given moneth and year
def generate_monthly_bill():
    credit_card_no = input("Enter the credit card number: ")
    month = input("Enter the month (MM): ")
    year = input("Enter the year (YYYY): ")

    transactions = credit_card_df.filter(
        (credit_card_df.CUST_CC_NO == credit_card_no) & 
        (credit_card_df.TIMEID.startswith(year + month))
    )
    
    total_amount = transactions.groupBy().sum('TRANSACTION_VALUE').collect()[0][0]

    print(f"Monthly bill for {credit_card_no} in {month}/{year} is: ${total_amount}")

# Dispaly customer's transactions between two dates
def transactions_between_dates():
    ssn = input("Enter the SSN of the customer: ")
    start_date = input("Enter the start date (YYYYMMDD): ")
    end_date = input("Enter the end date (YYYYMMDD): ")

    transactions = credit_card_df.filter(
        (credit_card_df.CUST_SSN == ssn) & 
        (credit_card_df.TIMEID.between(start_date, end_date))
    ).orderBy(credit_card_df.TIMEID.desc())

    transactions.show()

def main():
    while True:
        print("\nMain Menu:")
        print("1. Transaction Details Module")
        print("2. Customer Details Module")
        choice = input("Enter your choice: ")

        if choice == '1':
            print("\nTransaction Details Module:")
            print("1. Display transactions by zip, month, and year.")
            print("2. Display transactions by type.")
            print("3. Display transactions by branch state.")
            choice = input("Enter your choice: ")

            if choice == '1':
                transaction_by_zip_month_year()
            elif choice == '2':
                transaction_by_type()
            elif choice == '3':
                transaction_by_branch_state()

        elif choice == '2':
            print("\nCustomer Details Module:")
            print("1. Check existing account details.")
            print("2. Modify existing account details.")
            print("3. Generate monthly bill.")
            print("4. Display transactions between two dates.")
            choice = input("Enter your choice: ")

            if choice == '1':
                check_existing_account()
            elif choice == '2':
                modify_account_details()
            elif choice == '3':
                generate_monthly_bill()
            elif choice == '4':
                transactions_between_dates()

        else:
            print("Invalid choice. Please try again.")

        cont = input("\nDo you want to continue? (yes/no): ")
        if cont.lower() != 'yes':
            break

# Ensures that if script is run in main mondule, it will execute the main() function and present menu to user
if __name__ == '__main__':
    main()