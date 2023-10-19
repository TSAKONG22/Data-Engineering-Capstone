# Data-Engineering-Capstone Project
# Tae Sakong

### Project Overview

    The purpose of this project is to manage an ETL process for a Loan Application dataset and a Credit Card Dataset
    by using Python (Pandas, Matplotlib, analytics/visulization libraries), SQL, and Apache Spark 
    (Spark Core, SparkSQL,PySpark).

    The following are the project requirements:

    1. Project is uploaded to Github Repository (minimum of 1 branch)
    2. Virtual Environment with requirements.txt file
    3. Minimum of 3 commits
    4. Readme.md file dcoumenting the project details along with technical challenges
    5. Python codes, PySpark codes, database scripts, and databases are uploaded to the GitHub repository
    6. Include a screenshot of all graphs

    As a result, the following are the objectives of this project:

    1. Load Credit Card Database
        a. Create a python and PySpark SQL program ("Credit Card System") to read and extract JSON files according to the mapping document.
            . CDW_SAPP_BRANCH.JSON
            . CDW_SAPP_CREDITCARD.JSON
            . CDW_SAPP_CUSTOMER>JSON
        b. Load the data into RDBMS(SQL), to perform:
            . Create a database in MySQL, "creditcard_capstone"
            . Create a Python and PySpark program to load/write the "Credit Card System" into the RDBMS.
    2. Create a console-based Python program that performs the following for Transcation Details and Customer Details:
        a. Transaction Details
            . Display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order
            . Display the number and total values of a transcation for a given type
            . Display the total number and total values of transactions for branches in a given state
        b. Customer Details
            . Check the existing account details of a customer
            . Modify the existing accoutn details of a customer
            . Generate a monthly bill for a credit card number for a given month and year
            . Display the transactions made by a customer between two dates. Order by year, month, and day in descending order
    3. Visualize the data for analysis
        . Plot the transaction type with the highest transaction count
        . Plot the state with the highest number of customers
        . Plot the sum of all transactions for the top 10 customers, and which customer has the highets transaction amount
    4. Create access to Loan API Endpoint
        . Create a python program to GET(consume) data from the API endpoint for the loan application dataset
        . Find the status code of the API endpoint
        . Use PySpark to load data into RDBMS(SQL), "CDW-SAPP_loan_application"
    5. Visualize and analyze loan application data:
        . Plot the percentage of applications approved for self-employed applicants
        . Find the percentage of rejection for married male applicants
        . Plot the top three months with the largest volumne of transaction data
        . Plot the branch that processed the highest total dollar value of healthcare transactions
### Data

A. Credit Card Dataset

B. Loan Application Dataset


## Project Details

Directory Structure

## Technical Challenges
1.
2.
3.
4.
5.