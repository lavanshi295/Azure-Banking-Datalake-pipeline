# Purpose: Transform Bronze data to Silver layer

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

print("="*70)
print("SILVER LAYER TRANSFORMATIONS - BANKING DATASETS")
print("="*70)

storage_account = {Your_storage_account}
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/"
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"

# ============================================
# 1. CUSTOMERS TABLE - Silver Layer
# ============================================
print("\n1.1 Processing Customers Data (Bronze → Silver)...")

bronze_customers_path = f"{bronze_base_path}customers/"
df_customers_bronze = spark.read.format("delta").load(bronze_customers_path)
print(f"  Bronze records: {df_customers_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_customers_bronze = df_customers_bronze \
    .withColumn("annual_income", col("annual_income").cast("decimal(18,2)"))

df_customers_silver = df_customers_bronze \
    .dropDuplicates(["customer_id"]) \
    .filter(col("customer_id").isNotNull()) \
    .withColumn("age", floor(datediff(current_date(), col("dob")) / 365.25)) \
    .withColumn("age_group",
        when(col("age") < 25, "Young (<25)")
        .when(col("age") < 40, "Adult (25-39)")
        .when(col("age") < 60, "Middle Age (40-59)")
        .otherwise("Senior (60+)")) \
    .withColumn("full_name", concat(initcap(col("first_name")), lit(" "), initcap(col("last_name")))) \
    .withColumn("gender_standardized",
        when(upper(col("gender")).isin("M", "MALE"), "Male")
        .when(upper(col("gender")).isin("F", "FEMALE"), "Female")
        .otherwise("Other")) \
    .withColumn("income_bracket",
        when(col("annual_income") < 500000, "Low (<5L)")
        .when(col("annual_income") < 1500000, "Medium (5L-15L)")
        .when(col("annual_income") < 3000000, "High (15L-30L)")
        .otherwise("Very High (>30L)")) \
    .withColumn("email_domain", regexp_extract(col("email"), "@(.+)", 1)) \
    .withColumn("customer_tenure_years", floor(datediff(current_date(), col("customer_since")) / 365.25)) \
    .withColumn("is_kyc_verified", col("kyc_status") == "Verified") \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_customers_silver.count()}")
display(df_customers_silver.limit(3))

silver_customers_path = f"{silver_base_path}customers_silver/"
df_customers_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_customers_path)
print(f"  ✓ Saved to: {silver_customers_path}")

# ============================================
# 2. ACCOUNTS TABLE - Silver Layer
# ============================================
print("\n1.2 Processing Accounts Data (Bronze → Silver)...")

bronze_accounts_path = f"{bronze_base_path}accounts/"
df_accounts_bronze = spark.read.format("delta").load(bronze_accounts_path)
print(f"  Bronze records: {df_accounts_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_accounts_bronze = df_accounts_bronze \
    .withColumn("balance", col("balance").cast("decimal(18,2)"))

df_accounts_silver = df_accounts_bronze \
    .dropDuplicates(["account_id"]) \
    .filter(col("account_id").isNotNull()) \
    .withColumn("account_status_standardized",
        when(upper(col("status")).isin("ACTIVE"), "Active")
        .when(upper(col("status")).isin("INACTIVE"), "Inactive")
        .when(upper(col("status")).isin("FROZEN"), "Frozen")
        .when(upper(col("status")).isin("CLOSED"), "Closed")
        .otherwise("Unknown")) \
    .withColumn("is_active", col("account_status_standardized") == "Active") \
    .withColumn("balance_category",
        when(col("balance") < 50000, "Low (<50K)")
        .when(col("balance") < 200000, "Medium (50K-200K)")
        .when(col("balance") < 500000, "High (200K-500K)")
        .otherwise("Very High (>500K)")) \
    .withColumn("has_nominee", col("nominee_registered") == "Yes") \
    .withColumn("account_age_days", datediff(current_date(), col("open_date"))) \
    .withColumn("account_age_years", floor(col("account_age_days") / 365.25)) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_accounts_silver.count()}")
display(df_accounts_silver.limit(3))

silver_accounts_path = f"{silver_base_path}accounts_silver/"
df_accounts_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_accounts_path)
print(f"  ✓ Saved to: {silver_accounts_path}")

# ============================================
# 3. TRANSACTIONS TABLE - Silver Layer
# ============================================
print("\n1.3 Processing Transactions Data (Bronze → Silver)...")

bronze_transactions_path = f"{bronze_base_path}transactions/"
df_transactions_bronze = spark.read.format("delta").load(bronze_transactions_path)
print(f"  Bronze records: {df_transactions_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_transactions_bronze = df_transactions_bronze \
    .withColumn("amount", col("amount").cast("decimal(18,2)"))

df_transactions_silver = df_transactions_bronze \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_id").isNotNull()) \
    .withColumn("transaction_year", year(col("transaction_date"))) \
    .withColumn("transaction_month", month(col("transaction_date"))) \
    .withColumn("transaction_day", dayofmonth(col("transaction_date"))) \
    .withColumn("transaction_quarter", quarter(col("transaction_date"))) \
    .withColumn("transaction_week", weekofyear(col("transaction_date"))) \
    .withColumn("transaction_dayofweek", dayofweek(col("transaction_date"))) \
    .withColumn("day_name",
        when(col("transaction_dayofweek") == 1, "Sunday")
        .when(col("transaction_dayofweek") == 2, "Monday")
        .when(col("transaction_dayofweek") == 3, "Tuesday")
        .when(col("transaction_dayofweek") == 4, "Wednesday")
        .when(col("transaction_dayofweek") == 5, "Thursday")
        .when(col("transaction_dayofweek") == 6, "Friday")
        .otherwise("Saturday")) \
    .withColumn("amount_category",
        when(col("amount") < 1000, "Micro (<1K)")
        .when(col("amount") < 10000, "Small (1K-10K)")
        .when(col("amount") < 50000, "Medium (10K-50K)")
        .when(col("amount") < 100000, "Large (50K-100K)")
        .otherwise("Very Large (>100K)")) \
    .withColumn("is_debit", col("transaction_type") == "Debit") \
    .withColumn("is_credit", col("transaction_type") == "Credit") \
    .withColumn("is_success", col("status") == "Success") \
    .withColumn("is_failed", col("status") == "Failed") \
    .withColumn("is_reversed", col("status") == "Reversed") \
    .withColumn("payment_mode_standardized",
        when(upper(col("payment_mode")).isin("UPI"), "UPI")
        .when(upper(col("payment_mode")).isin("NETBANKING"), "Net Banking")
        .when(upper(col("payment_mode")).isin("RTGS"), "RTGS")
        .when(upper(col("payment_mode")).isin("IMPS"), "IMPS")
        .when(upper(col("payment_mode")).isin("NEFT"), "NEFT")
        .when(upper(col("payment_mode")).isin("ATM"), "ATM")
        .when(upper(col("payment_mode")).isin("CHEQUE"), "Cheque")
        .otherwise("Other")) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_transactions_silver.count()}")
print(f"  Total transaction value: ₹{df_transactions_silver.agg(sum('amount')).collect()[0][0]:,.2f}")
display(df_transactions_silver.limit(3))

silver_transactions_path = f"{silver_base_path}transactions_silver/"
df_transactions_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_transactions_path)
print(f"  ✓ Saved to: {silver_transactions_path}")

# ============================================
# 4. LOANS TABLE - Silver Layer
# ============================================
print("\n1.4 Processing Loans Data (Bronze → Silver)...")

bronze_loans_path = f"{bronze_base_path}loans/"
df_loans_bronze = spark.read.format("delta").load(bronze_loans_path)
print(f"  Bronze records: {df_loans_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_loans_bronze = df_loans_bronze \
    .withColumn("loan_amount", col("loan_amount").cast("decimal(18,2)")) \
    .withColumn("interest_rate", col("interest_rate").cast("decimal(5,2)")) \
    .withColumn("tenure_months", col("tenure_months").cast("integer"))

df_loans_silver = df_loans_bronze \
    .dropDuplicates(["loan_id"]) \
    .filter(col("loan_id").isNotNull()) \
    .withColumn("loan_status_standardized",
        when(upper(col("loan_status")).isin("ACTIVE"), "Active")
        .when(upper(col("loan_status")).isin("CLOSED"), "Closed")
        .when(upper(col("loan_status")).isin("DEFAULTED"), "Defaulted")
        .when(upper(col("loan_status")).isin("UNDER REVIEW"), "Under Review")
        .otherwise("Unknown")) \
    .withColumn("is_active_loan", col("loan_status_standardized") == "Active") \
    .withColumn("is_defaulted", col("loan_status_standardized") == "Defaulted") \
    .withColumn("total_interest", col("loan_amount") * col("interest_rate") / 100 * col("tenure_months") / 12) \
    .withColumn("total_payable", col("loan_amount") + col("total_interest")) \
    .withColumn("interest_rate_category",
        when(col("interest_rate") < 10, "Low (<10%)")
        .when(col("interest_rate") < 15, "Medium (10-15%)")
        .when(col("interest_rate") < 18, "High (15-18%)")
        .otherwise("Very High (>18%)")) \
    .withColumn("loan_amount_category",
        when(col("loan_amount") < 500000, "Small (<5L)")
        .when(col("loan_amount") < 2000000, "Medium (5L-20L)")
        .when(col("loan_amount") < 5000000, "Large (20L-50L)")
        .otherwise("Very Large (>50L)")) \
    .withColumn("has_collateral", col("collateral_required") == "Yes") \
    .withColumn("loan_age_months", floor(datediff(current_date(), col("loan_start_date")) / 30.44)) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_loans_silver.count()}")
print(f"  Total loan amount: ₹{df_loans_silver.agg(sum('loan_amount')).collect()[0][0]:,.2f}")
display(df_loans_silver.limit(3))

silver_loans_path = f"{silver_base_path}loans_silver/"
df_loans_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_loans_path)
print(f"  ✓ Saved to: {silver_loans_path}")

# ============================================
# 5. CREDIT CARDS TABLE - Silver Layer
# ============================================
print("\n1.5 Processing Credit Cards Data (Bronze → Silver)...")

bronze_cards_path = f"{bronze_base_path}credit_cards/"
df_cards_bronze = spark.read.format("delta").load(bronze_cards_path)
print(f"  Bronze records: {df_cards_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_cards_bronze = df_cards_bronze \
    .withColumn("credit_limit", col("credit_limit").cast("decimal(18,2)")) \
    .withColumn("outstanding_balance", col("outstanding_balance").cast("decimal(18,2)")) \
    .withColumn("available_limit", col("available_limit").cast("decimal(18,2)"))

df_cards_silver = df_cards_bronze \
    .dropDuplicates(["card_id"]) \
    .filter(col("card_id").isNotNull()) \
    .withColumn("card_status_standardized",
        when(upper(col("card_status")).isin("ACTIVE"), "Active")
        .when(upper(col("card_status")).isin("CLOSED"), "Closed")
        .when(upper(col("card_status")).isin("EXPIRED"), "Expired")
        .when(upper(col("card_status")).isin("BLOCKED"), "Blocked")
        .otherwise("Unknown")) \
    .withColumn("is_active_card", col("card_status_standardized") == "Active") \
    .withColumn("credit_utilization_pct", round(col("outstanding_balance") / col("credit_limit") * 100, 2)) \
    .withColumn("utilization_category",
        when(col("credit_utilization_pct") < 30, "Low (<30%)")
        .when(col("credit_utilization_pct") < 60, "Medium (30-60%)")
        .when(col("credit_utilization_pct") < 90, "High (60-90%)")
        .otherwise("Critical (>90%)")) \
    .withColumn("card_type_standardized",
        when(upper(col("card_type")).isin("CLASSIC"), "Classic")
        .when(upper(col("card_type")).isin("GOLD"), "Gold")
        .when(upper(col("card_type")).isin("PLATINUM"), "Platinum")
        .when(upper(col("card_type")).isin("SIGNATURE"), "Signature")
        .when(upper(col("card_type")).isin("BUSINESS"), "Business")
        .otherwise("Other")) \
    .withColumn("credit_limit_category",
        when(col("credit_limit") < 100000, "Basic (<1L)")
        .when(col("credit_limit") < 250000, "Standard (1L-2.5L)")
        .when(col("credit_limit") < 500000, "Premium (2.5L-5L)")
        .otherwise("Elite (>5L)")) \
    .withColumn("available_limit_pct", round(col("available_limit") / col("credit_limit") * 100, 2)) \
    .withColumn("card_age_days", datediff(current_date(), col("issue_date"))) \
    .withColumn("days_to_expiry", datediff(col("expiry_date"), current_date())) \
    .withColumn("is_near_expiry", col("days_to_expiry") < 90) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_cards_silver.count()}")
print(f"  Average utilization: {df_cards_silver.agg(avg('credit_utilization_pct')).collect()[0][0]:.2f}%")
display(df_cards_silver.limit(3))

silver_cards_path = f"{silver_base_path}credit_cards_silver/"
df_cards_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_cards_path)
print(f"  ✓ Saved to: {silver_cards_path}")

# ============================================
# 6. BRANCHES TABLE - Silver Layer
# ============================================
print("\n1.6 Processing Branches Data (Bronze → Silver)...")

bronze_branches_path = f"{bronze_base_path}branches/"
df_branches_bronze = spark.read.format("delta").load(bronze_branches_path)
print(f"  Bronze records: {df_branches_bronze.count()}")

df_branches_silver = df_branches_bronze \
    .dropDuplicates(["branch_id"]) \
    .filter(col("branch_id").isNotNull()) \
    .withColumn("branch_name_clean", initcap(trim(col("branch_name")))) \
    .withColumn("city_clean", initcap(trim(col("city")))) \
    .withColumn("state_clean", initcap(trim(col("state")))) \
    .withColumn("manager_name_clean", initcap(trim(col("manager_name")))) \
    .withColumn("branch_age_years", floor(datediff(current_date(), col("open_date")) / 365.25)) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_branches_silver.count()}")
display(df_branches_silver.limit(3))

silver_branches_path = f"{silver_base_path}branches_silver/"
df_branches_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_branches_path)
print(f"  ✓ Saved to: {silver_branches_path}")

# ============================================
# 7. EMPLOYEES TABLE - Silver Layer
# ============================================
print("\n1.7 Processing Employees Data (Bronze → Silver)...")

bronze_employees_path = f"{bronze_base_path}employees/"
df_employees_bronze = spark.read.format("delta").load(bronze_employees_path)
print(f"  Bronze records: {df_employees_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_employees_bronze = df_employees_bronze \
    .withColumn("salary", col("salary").cast("decimal(18,2)"))

df_employees_silver = df_employees_bronze \
    .dropDuplicates(["employee_id"]) \
    .filter(col("employee_id").isNotNull()) \
    .withColumn("employee_name_clean", initcap(trim(col("employee_name")))) \
    .withColumn("first_name", split(col("employee_name_clean"), " ")[0]) \
    .withColumn("last_name", split(col("employee_name_clean"), " ")[1]) \
    .withColumn("designation_clean", initcap(trim(col("designation")))) \
    .withColumn("salary_bracket",
        when(col("salary") < 50000, "Entry (<50K)")
        .when(col("salary") < 100000, "Junior (50K-1L)")
        .when(col("salary") < 150000, "Mid (1L-1.5L)")
        .otherwise("Senior (>1.5L)")) \
    .withColumn("is_active_employee", col("employment_status") == "Active") \
    .withColumn("tenure_years", floor(datediff(current_date(), col("joining_date")) / 365.25)) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_employees_silver.count()}")
display(df_employees_silver.limit(3))

silver_employees_path = f"{silver_base_path}employees_silver/"
df_employees_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_employees_path)
print(f"  ✓ Saved to: {silver_employees_path}")

# ============================================
# 8. FRAUD TABLE - Silver Layer
# ============================================
print("\n1.8 Processing Fraud Data (Bronze → Silver)...")

bronze_fraud_path = f"{bronze_base_path}fraud_transactions/"
df_fraud_bronze = spark.read.format("delta").load(bronze_fraud_path)
print(f"  Bronze records: {df_fraud_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_fraud_bronze = df_fraud_bronze \
    .withColumn("loss_amount", col("loss_amount").cast("decimal(18,2)")) \
    .withColumn("risk_score", col("risk_score").cast("integer"))

df_fraud_silver = df_fraud_bronze \
    .dropDuplicates(["fraud_id"]) \
    .filter(col("fraud_id").isNotNull()) \
    .withColumn("risk_level",
        when(col("risk_score") >= 90, "Critical")
        .when(col("risk_score") >= 70, "High")
        .when(col("risk_score") >= 50, "Medium")
        .otherwise("Low")) \
    .withColumn("is_confirmed_fraud", col("investigation_status") == "Confirmed Fraud") \
    .withColumn("is_false_positive", col("investigation_status") == "False Positive") \
    .withColumn("is_open_investigation", col("investigation_status").isin("Open", "In Progress")) \
    .withColumn("loss_category",
        when(col("loss_amount") < 10000, "Minor (<10K)")
        .when(col("loss_amount") < 50000, "Moderate (10K-50K)")
        .when(col("loss_amount") < 100000, "Significant (50K-1L)")
        .otherwise("Major (>1L)")) \
    .withColumn("detection_delay_days", datediff(current_date(), col("detected_date"))) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_fraud_silver.count()}")
print(f"  Total loss amount: ₹{df_fraud_silver.agg(sum('loss_amount')).collect()[0][0]:,.2f}")
display(df_fraud_silver.limit(3))

silver_fraud_path = f"{silver_base_path}fraud_transactions_silver/"
df_fraud_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_fraud_path)
print(f"  ✓ Saved to: {silver_fraud_path}")

# ============================================
# 9. INSURANCE TABLE - Silver Layer
# ============================================
print("\n1.9 Processing Insurance Data (Bronze → Silver)...")

bronze_insurance_path = f"{bronze_base_path}insurance_products/"
df_insurance_bronze = spark.read.format("delta").load(bronze_insurance_path)
print(f"  Bronze records: {df_insurance_bronze.count()}")

# FIX: Cast numeric STRING columns to proper types
df_insurance_bronze = df_insurance_bronze \
    .withColumn("premium_amount", col("premium_amount").cast("decimal(18,2)")) \
    .withColumn("sum_assured", col("sum_assured").cast("decimal(18,2)"))

df_insurance_silver = df_insurance_bronze \
    .dropDuplicates(["policy_id"]) \
    .filter(col("policy_id").isNotNull()) \
    .withColumn("policy_type_standardized",
        when(upper(col("policy_type")).isin("LIFE INSURANCE"), "Life")
        .when(upper(col("policy_type")).isin("HEALTH INSURANCE"), "Health")
        .when(upper(col("policy_type")).isin("VEHICLE INSURANCE"), "Vehicle")
        .when(upper(col("policy_type")).isin("TRAVEL INSURANCE"), "Travel")
        .when(upper(col("policy_type")).isin("ACCIDENT INSURANCE"), "Accident")
        .otherwise("Other")) \
    .withColumn("is_active_policy", col("status") == "Active") \
    .withColumn("is_expired", col("status") == "Expired") \
    .withColumn("premium_category",
        when(col("premium_amount") < 25000, "Low (<25K)")
        .when(col("premium_amount") < 75000, "Medium (25K-75K)")
        .when(col("premium_amount") < 150000, "High (75K-1.5L)")
        .otherwise("Premium (>1.5L)")) \
    .withColumn("coverage_ratio", round(col("sum_assured") / col("premium_amount"), 2)) \
    .withColumn("policy_remaining_days", datediff(col("end_date"), current_date())) \
    .withColumn("is_near_expiry", col("policy_remaining_days") < 90) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_insurance_silver.count()}")
display(df_insurance_silver.limit(3))

silver_insurance_path = f"{silver_base_path}insurance_products_silver/"
df_insurance_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_insurance_path)
print(f"  ✓ Saved to: {silver_insurance_path}")

# ============================================
# 10. SUPPORT TICKETS TABLE - Silver Layer
# ============================================
print("\n1.10 Processing Support Tickets Data (Bronze → Silver)...")

bronze_tickets_path = f"{bronze_base_path}customer_support_tickets/"
df_tickets_bronze = spark.read.format("delta").load(bronze_tickets_path)
print(f"  Bronze records: {df_tickets_bronze.count()}")

df_tickets_silver = df_tickets_bronze \
    .dropDuplicates(["ticket_id"]) \
    .filter(col("ticket_id").isNotNull()) \
    .withColumn("priority_level",
        when(upper(col("priority")) == "CRITICAL", 3)
        .when(upper(col("priority")) == "HIGH", 2)
        .when(upper(col("priority")) == "MEDIUM", 1)
        .otherwise(0)) \
    .withColumn("is_resolved", col("status") == "Resolved") \
    .withColumn("is_open", col("status").isin("Open", "In Progress")) \
    .withColumn("resolution_time_days",
        when(col("resolved_date").isNotNull(), datediff(col("resolved_date"), col("created_date")))
        .otherwise(None)) \
    .withColumn("resolution_time_category",
        when(col("resolution_time_days") <= 1, "Same Day")
        .when(col("resolution_time_days") <= 3, "1-3 Days")
        .when(col("resolution_time_days") <= 7, "3-7 Days")
        .when(col("resolution_time_days") <= 30, "1-4 Weeks")
        .otherwise(">1 Month")) \
    .withColumn("channel_standardized", initcap(trim(col("channel")))) \
    .withColumn("issue_type_standardized",
        when(upper(col("issue_type")).isin("CARD BLOCK"), "Card Block")
        .when(upper(col("issue_type")).isin("CHARGEBACK"), "Chargeback")
        .when(upper(col("issue_type")).isin("KYC UPDATE"), "KYC Update")
        .when(upper(col("issue_type")).isin("LOAN QUERY"), "Loan Query")
        .when(upper(col("issue_type")).isin("FRAUD COMPLAINT"), "Fraud Complaint")
        .when(upper(col("issue_type")).isin("ACCOUNT ACCESS"), "Account Access")
        .when(upper(col("issue_type")).isin("FAILED TRANSACTION"), "Failed Transaction")
        .when(upper(col("issue_type")).isin("STATEMENT REQUEST"), "Statement Request")
        .otherwise("Other")) \
    .withColumn("is_critical", col("priority") == "Critical") \
    .withColumn("is_high_priority", col("priority").isin("Critical", "High")) \
    .withColumn("silver_processing_timestamp", current_timestamp()) \
    .withColumn("silver_batch_id", lit(datetime.now().strftime("%Y%m%d_%H%M%S"))) \
    .drop("ingestion_timestamp", "source_file", "ingestion_batch_id")

print(f"  Silver records: {df_tickets_silver.count()}")
display(df_tickets_silver.limit(3))

silver_tickets_path = f"{silver_base_path}customer_support_tickets_silver/"
df_tickets_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_tickets_path)
print(f"  ✓ Saved to: {silver_tickets_path}")

print("\n" + "="*60)
print("✅ SILVER LAYER COMPLETED - All 10 tables saved to ADLS")
print("="*60)