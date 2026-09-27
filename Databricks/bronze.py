# Purpose: Write all banking datasets as Delta tables to Bronze container

from pyspark.sql.functions import input_file_name, current_timestamp, lit
from pyspark.sql.types import *
from datetime import datetime

print("="*60)
print("CREATING BRONZE DELTA TABLES FOR ALL DATASETS")
print("="*60)

storage_account = {Your_storage_account}
raw_base_path = f"abfss://raw@{storage_account}.dfs.core.windows.net/"
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"

# Read and Write each table as Delta

# 1. Customers Table
print("Processing Customers...")
df_customers = spark.read.option("header", True).csv(f"{raw_base_path}customers.csv")
df_customers_with_metadata = df_customers \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("customers.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_customers_path = f"{bronze_base_path}customers/"
df_customers_with_metadata.write.format("delta").mode("overwrite").save(bronze_customers_path)
print(f"✓ Customers Delta table created")

# 2. Accounts Table
print("Processing Accounts...")
df_accounts = spark.read.option("header", True).csv(f"{raw_base_path}accounts.csv")
df_accounts_with_metadata = df_accounts \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("accounts.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_accounts_path = f"{bronze_base_path}accounts/"
df_accounts_with_metadata.write.format("delta").mode("overwrite").save(bronze_accounts_path)
print(f"✓ Accounts Delta table created")

# 3. Transactions Table
print("Processing Transactions...")
df_transactions = spark.read.option("header", True).csv(f"{raw_base_path}transactions.csv")
df_transactions_with_metadata = df_transactions \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("transactions.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_transactions_path = f"{bronze_base_path}transactions/"
df_transactions_with_metadata.write.format("delta").mode("overwrite").save(bronze_transactions_path)
print(f"✓ Transactions Delta table created")

# 4. Loans Table
print("Processing Loans...")
df_loans = spark.read.option("header", True).csv(f"{raw_base_path}loans.csv")
df_loans_with_metadata = df_loans \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("loans.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_loans_path = f"{bronze_base_path}loans/"
df_loans_with_metadata.write.format("delta").mode("overwrite").save(bronze_loans_path)
print(f"✓ Loans Delta table created")

# 5. Credit Cards Table
print("Processing Credit Cards...")
df_cards = spark.read.option("header", True).csv(f"{raw_base_path}credit_cards.csv")
df_cards_with_metadata = df_cards \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("credit_cards.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_cards_path = f"{bronze_base_path}credit_cards/"
df_cards_with_metadata.write.format("delta").mode("overwrite").save(bronze_cards_path)
print(f"✓ Credit Cards Delta table created")

# 6. Branches Table
print("Processing Branches...")
df_branches = spark.read.option("header", True).csv(f"{raw_base_path}branches.csv")
df_branches_with_metadata = df_branches \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("branches.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_branches_path = f"{bronze_base_path}branches/"
df_branches_with_metadata.write.format("delta").mode("overwrite").save(bronze_branches_path)
print(f"✓ Branches Delta table created")

# 7. Employees Table
print("Processing Employees...")
df_employees = spark.read.option("header", True).csv(f"{raw_base_path}employees.csv")
df_employees_with_metadata = df_employees \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("employees.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_employees_path = f"{bronze_base_path}employees/"
df_employees_with_metadata.write.format("delta").mode("overwrite").save(bronze_employees_path)
print(f"✓ Employees Delta table created")

# 8. Fraud Table
print("Processing Fraud...")
df_fraud = spark.read.option("header", True).csv(f"{raw_base_path}fraud_transactions.csv")
df_fraud_with_metadata = df_fraud \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("fraud_transactions.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_fraud_path = f"{bronze_base_path}fraud_transactions/"
df_fraud_with_metadata.write.format("delta").mode("overwrite").save(bronze_fraud_path)
print(f"✓ Fraud Delta table created")

# 9. Insurance Table
print("Processing Insurance...")
df_insurance = spark.read.option("header", True).csv(f"{raw_base_path}insurance_products.csv")
df_insurance_with_metadata = df_insurance \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("insurance_products.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_insurance_path = f"{bronze_base_path}insurance_products/"
df_insurance_with_metadata.write.format("delta").mode("overwrite").save(bronze_insurance_path)
print(f"✓ Insurance Delta table created")

# 10. Support Tickets Table
print("Processing Support Tickets...")
df_tickets = spark.read.option("header", True).csv(f"{raw_base_path}customer_support_tickets.csv")
df_tickets_with_metadata = df_tickets \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("customer_support_tickets.csv")) \
    .withColumn("ingestion_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S")))
bronze_tickets_path = f"{bronze_base_path}customer_support_tickets/"
df_tickets_with_metadata.write.format("delta").mode("overwrite").save(bronze_tickets_path)
print(f"✓ Support Tickets Delta table created")

print("\n✅ ALL BRONZE DELTA TABLES CREATED SUCCESSFULLY!")