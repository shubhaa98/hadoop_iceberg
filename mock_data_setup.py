from pyspark.sql import SparkSession
from datetime import datetime

# Initialize Spark
# spark = SparkSession.builder.appName("Mock_Banking_Data_Setup").enableHiveSupport().getOrCreate()
spark = (
    SparkSession.builder
    .appName("Mock_Banking_Data_Setup")
    .master("local[*]")          # run locally using all cores
    .getOrCreate()
)


print("Creating mock databases and tables...")

# Create databases
spark.sql("CREATE DATABASE IF NOT EXISTS staging_db")
spark.sql("CREATE DATABASE IF NOT EXISTS core_db")
spark.sql("CREATE DATABASE IF NOT EXISTS final_db")

# ---------------------------
# Staging Layer Tables
# ---------------------------

# Customer
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer AS
SELECT * FROM VALUES
  (1, 'Alice', 'Retail'),
  (2, 'Bob', 'Wealth'),
  (3, 'Charlie', NULL)
AS t(customer_id, name, segment)
""")
spark.table("customer").write.mode("overwrite").saveAsTable("staging_db.customer")

# Account
spark.sql("""
CREATE OR REPLACE TEMP VIEW account AS
SELECT * FROM VALUES
  (101, 1, 'Checking', 'Active'),
  (102, 1, 'Savings', 'Active'),
  (201, 2, 'Checking', 'Inactive'),
  (301, 3, 'Loan', 'Active')
AS t(account_id, customer_id, account_type, status)
""")
spark.table("account").write.mode("overwrite").saveAsTable("staging_db.account")

# Transactions
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW transaction AS
SELECT * FROM VALUES
  (101, 'T1', 200.00, 'DEBIT', date_sub(current_date(), 5), 9001),
  (101, 'T2', 500.00, 'CREDIT', date_sub(current_date(), 2), 9002),
  (201, 'T3', 150.00, 'DEBIT', date_sub(current_date(), 10), 9003)
AS t(account_id, txn_id, amount, txn_type, txn_date, merchant_id)
""")
spark.table("transaction").write.mode("overwrite").saveAsTable("staging_db.transaction")

# ---------------------------
# Core Layer Tables
# ---------------------------

# Product Dimension
spark.sql("""
CREATE OR REPLACE TEMP VIEW product_dim AS
SELECT * FROM VALUES
  (101, 'Everyday Checking', 'Deposit'),
  (102, 'High Yield Savings', 'Deposit'),
  (301, 'Personal Loan', 'Credit')
AS t(account_id, product_name, product_category)
""")
spark.table("product_dim").write.mode("overwrite").saveAsTable("core_db.product_dim")

# Branch Dimension
spark.sql("""
CREATE OR REPLACE TEMP VIEW branch_dim AS
SELECT * FROM VALUES
  (101, 'Downtown Branch', 'NYC'),
  (102, 'Uptown Branch', 'NYC'),
  (201, 'Jersey Branch', 'NJ'),
  (301, 'Queens Branch', 'NYC')
AS t(account_id, branch_name, branch_region)
""")
spark.table("branch_dim").write.mode("overwrite").saveAsTable("core_db.branch_dim")

# Merchant Dimension
spark.sql("""
CREATE OR REPLACE TEMP VIEW merchant_dim AS
SELECT * FROM VALUES
  (9001, 'Grocery'),
  (9002, 'Electronics'),
  (9003, 'Dining')
AS t(merchant_id, merchant_category)
""")
spark.table("merchant_dim").write.mode("overwrite").saveAsTable("core_db.merchant_dim")

# Segmentation rules
spark.sql("""
CREATE OR REPLACE TEMP VIEW segmentation_rules AS
SELECT * FROM VALUES
  (1, 'Mass Market'),
  (2, 'Affluent')
AS t(customer_id, segment_name)
""")
spark.table("segmentation_rules").write.mode("overwrite").saveAsTable("core_db.segmentation_rules")

# Customer Region
spark.sql("""
CREATE OR REPLACE TEMP VIEW customer_region AS
SELECT * FROM VALUES
  (1, 'East', 'Y'),
  (2, 'West', 'Y'),
  (3, 'South', 'N')
AS t(cust_id, region, active_flag)
""")
spark.table("customer_region").write.mode("overwrite").saveAsTable("core_db.customer_region")

# Risk Assessment
today = datetime.now().strftime("%Y-%m-%d")
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW risk_assessment AS
SELECT * FROM VALUES
  (1, 0.7, '{today}'),
  (2, 0.3, '{today}')
AS t(customer_id, score, as_of_date)
""")
spark.table("risk_assessment").write.mode("overwrite").saveAsTable("core_db.risk_assessment")

print("Mock data created successfully!")
