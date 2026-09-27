# Purpose: Transform Silver data to Gold layer for business intelligence

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from datetime import datetime

print("="*70)
print("GOLD LAYER TRANSFORMATIONS - BUSINESS AGGREGATIONS")
print("="*70)

storage_account = {Your_storage_account}
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/"

# Read Silver tables
print("\nReading Silver tables...")
df_customers_silver = spark.read.format("delta").load(f"{silver_base_path}customers_silver/")
df_accounts_silver = spark.read.format("delta").load(f"{silver_base_path}accounts_silver/")
df_transactions_silver = spark.read.format("delta").load(f"{silver_base_path}transactions_silver/")
df_loans_silver = spark.read.format("delta").load(f"{silver_base_path}loans_silver/")
df_cards_silver = spark.read.format("delta").load(f"{silver_base_path}credit_cards_silver/")
df_branches_silver = spark.read.format("delta").load(f"{silver_base_path}branches_silver/")
df_employees_silver = spark.read.format("delta").load(f"{silver_base_path}employees_silver/")
df_fraud_silver = spark.read.format("delta").load(f"{silver_base_path}fraud_transactions_silver/")
df_insurance_silver = spark.read.format("delta").load(f"{silver_base_path}insurance_products_silver/")
df_tickets_silver = spark.read.format("delta").load(f"{silver_base_path}customer_support_tickets_silver/")

print("✓ All Silver tables loaded")

# ============================================
# 1. CUSTOMER 360 AGGREGATION (Gold)
# ============================================
print("\n2.1 Creating Customer 360 Aggregate View...")

df_customer_360 = df_customers_silver \
    .join(df_accounts_silver.groupBy("customer_id").agg(
        count("account_id").alias("total_accounts"),
        sum(when(col("is_active"), 1).otherwise(0)).alias("active_accounts"),
        sum("balance").alias("total_balance"),
        avg("balance").alias("avg_balance")
    ), "customer_id", "left") \
    .join(df_loans_silver.groupBy("customer_id").agg(
        count("loan_id").alias("total_loans"),
        sum(when(col("is_active_loan"), 1).otherwise(0)).alias("active_loans"),
        sum("loan_amount").alias("total_loan_amount"),
        sum(when(col("is_defaulted"), 1).otherwise(0)).alias("defaulted_loans")
    ), "customer_id", "left") \
    .join(df_cards_silver.groupBy("customer_id").agg(
        count("card_id").alias("total_cards"),
        sum(when(col("is_active_card"), 1).otherwise(0)).alias("active_cards"),
        sum("credit_limit").alias("total_credit_limit"),
        sum("outstanding_balance").alias("total_card_outstanding"),
        avg("credit_utilization_pct").alias("avg_credit_utilization")
    ), "customer_id", "left") \
    .join(df_insurance_silver.groupBy("customer_id").agg(
        count("policy_id").alias("total_policies"),
        sum(when(col("is_active_policy"), 1).otherwise(0)).alias("active_policies"),
        sum("premium_amount").alias("total_premium")
    ), "customer_id", "left") \
    .join(df_tickets_silver.groupBy("customer_id").agg(
        count("ticket_id").alias("total_tickets"),
        sum(when(col("is_resolved"), 1).otherwise(0)).alias("resolved_tickets"),
        avg("resolution_time_days").alias("avg_resolution_days")
    ), "customer_id", "left") \
    .fillna(0) \
    .withColumn("total_assets", col("total_balance") + col("total_credit_limit") - col("total_card_outstanding")) \
    .withColumn("risk_score",
        when(col("defaulted_loans") > 0, "High")
        .when(col("avg_credit_utilization") > 80, "Medium-High")
        .when(col("avg_credit_utilization") > 50, "Medium")
        .otherwise("Low")) \
    .select(
        "customer_id", "full_name", "age_group", "income_bracket", "city", "state",
        "total_accounts", "active_accounts", "total_balance", "avg_balance",
        "total_loans", "active_loans", "total_loan_amount", "defaulted_loans",
        "total_cards", "active_cards", "total_credit_limit", "total_card_outstanding",
        "avg_credit_utilization", "total_policies", "active_policies", "total_premium",
        "total_tickets", "resolved_tickets", "avg_resolution_days", "total_assets", "risk_score"
    )

print(f"  Gold records: {df_customer_360.count()} customers")
display(df_customer_360.limit(5))

gold_customer360_path = f"{gold_base_path}customer_360/"
df_customer_360.write.format("delta").mode("overwrite").save(gold_customer360_path)
print(f"  ✓ Saved to: {gold_customer360_path}")

# ============================================
# 2. BRANCH PERFORMANCE METRICS (Gold)
# ============================================
print("\n2.2 Creating Branch Performance Metrics...")

# FIX: Loans and Transactions don't have branch_id directly.
# Route them via accounts (which has branch_id) using customer_id as bridge.

# Accounts aggregated at branch level
df_accounts_by_branch = df_accounts_silver.groupBy("branch_id").agg(
    count("account_id").alias("total_accounts"),
    sum("balance").alias("total_deposits"),
    avg("balance").alias("avg_deposit")
)

# Loans → join to accounts on customer_id to get branch_id, then aggregate
df_loans_by_branch = df_loans_silver \
    .join(df_accounts_silver.select("customer_id", "branch_id").dropDuplicates(["customer_id"]), "customer_id", "left") \
    .groupBy("branch_id").agg(
        count("loan_id").alias("total_loans"),
        sum("loan_amount").alias("total_loan_disbursed"),
        avg("interest_rate").alias("avg_interest_rate")
    )

# Transactions → join to accounts on account_id to get branch_id, then aggregate
df_transactions_by_branch = df_transactions_silver \
    .join(df_accounts_silver.select("account_id", "branch_id"), "account_id", "left") \
    .groupBy("branch_id").agg(
        count("transaction_id").alias("total_transactions"),
        sum(when(col("transaction_type") == "Debit", col("amount")).otherwise(0)).alias("total_debits"),
        sum(when(col("transaction_type") == "Credit", col("amount")).otherwise(0)).alias("total_credits")
    )

df_branch_performance = df_branches_silver \
    .join(df_accounts_by_branch, "branch_id", "left") \
    .join(df_employees_silver.filter(col("employment_status") == "Active").groupBy("branch_id").agg(
        count("employee_id").alias("total_employees"),
        avg("salary").alias("avg_salary")
    ), "branch_id", "left") \
    .join(df_loans_by_branch, "branch_id", "left") \
    .join(df_transactions_by_branch, "branch_id", "left") \
    .fillna(0) \
    .withColumn("net_flow", col("total_credits") - col("total_debits")) \
    .withColumn("employee_efficiency", round(col("total_accounts") / col("total_employees"), 2)) \
    .select(
        "branch_id", "branch_name_clean", "city_clean", "state_clean",
        "total_accounts", "total_deposits", "avg_deposit",
        "total_employees", "avg_salary", "employee_efficiency",
        "total_loans", "total_loan_disbursed", "avg_interest_rate",
        "total_transactions", "total_debits", "total_credits", "net_flow"
    )

print(f"  Gold records: {df_branch_performance.count()} branches")
display(df_branch_performance.limit(5))

gold_branch_path = f"{gold_base_path}branch_performance/"
df_branch_performance.write.format("delta").mode("overwrite").save(gold_branch_path)
print(f"  ✓ Saved to: {gold_branch_path}")

# ============================================
# 3. LOAN PORTFOLIO ANALYSIS (Gold)
# ============================================
print("\n2.3 Creating Loan Portfolio Analysis...")

df_loan_portfolio = df_loans_silver \
    .join(df_customers_silver.select("customer_id", "age_group", "income_bracket", "city"), "customer_id", "left") \
    .groupBy("loan_type", "loan_status_standardized", "age_group", "income_bracket") \
    .agg(
        count("loan_id").alias("loan_count"),
        sum("loan_amount").alias("total_loan_amount"),
        avg("loan_amount").alias("avg_loan_amount"),
        avg("interest_rate").alias("avg_interest_rate"),
        sum("total_interest").alias("total_interest_income"),
        sum("total_payable").alias("total_receivable")
    ) \
    .orderBy(col("total_loan_amount").desc())

print(f"  Gold records: {df_loan_portfolio.count()} loan segments")
display(df_loan_portfolio.limit(10))

gold_loan_path = f"{gold_base_path}loan_portfolio/"
df_loan_portfolio.write.format("delta").mode("overwrite").save(gold_loan_path)
print(f"  ✓ Saved to: {gold_loan_path}")

# ============================================
# 4. FRAUD ANALYSIS DASHBOARD (Gold)
# ============================================
print("\n2.4 Creating Fraud Analysis Dashboard...")

df_fraud_analysis = df_fraud_silver \
    .join(df_transactions_silver.select("transaction_id", "transaction_date", "amount", "payment_mode_standardized", "merchant_name"), "transaction_id", "left") \
    .withColumn("fraud_year", year(col("detected_date"))) \
    .withColumn("fraud_month", month(col("detected_date"))) \
    .groupBy("fraud_year", "fraud_month", "fraud_type", "risk_level") \
    .agg(
        count("fraud_id").alias("fraud_count"),
        sum("loss_amount").alias("total_loss"),
        avg("loss_amount").alias("avg_loss"),
        sum(when(col("is_confirmed_fraud"), 1).otherwise(0)).alias("confirmed_frauds"),
        sum(when(col("is_false_positive"), 1).otherwise(0)).alias("false_positives")
    ) \
    .withColumn("confirmation_rate", round(col("confirmed_frauds") / col("fraud_count") * 100, 2)) \
    .orderBy("fraud_year", "fraud_month")

print(f"  Gold records: {df_fraud_analysis.count()} fraud segments")
display(df_fraud_analysis.limit(10))

gold_fraud_path = f"{gold_base_path}fraud_analysis/"
df_fraud_analysis.write.format("delta").mode("overwrite").save(gold_fraud_path)
print(f"  ✓ Saved to: {gold_fraud_path}")

# ============================================
# 5. DAILY TRANSACTION TRENDS (Gold)
# ============================================
print("\n2.5 Creating Daily Transaction Trends...")

df_daily_trends = df_transactions_silver \
    .groupBy("transaction_date", "transaction_type", "payment_mode_standardized") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_transaction_amount"),
        sum(when(col("is_success"), 1).otherwise(0)).alias("successful_count"),
        sum(when(col("is_failed"), 1).otherwise(0)).alias("failed_count")
    ) \
    .withColumn("success_rate", round(col("successful_count") / col("transaction_count") * 100, 2)) \
    .orderBy("transaction_date")

print(f"  Gold records: {df_daily_trends.count()} daily records")
display(df_daily_trends.limit(10))

gold_daily_path = f"{gold_base_path}daily_transaction_trends/"
df_daily_trends.write.format("delta").mode("overwrite").save(gold_daily_path)
print(f"  ✓ Saved to: {gold_daily_path}")

# ============================================
# 6. CUSTOMER SUPPORT METRICS (Gold)
# ============================================
print("\n2.6 Creating Customer Support Metrics...")

df_support_metrics = df_tickets_silver \
    .groupBy("issue_type_standardized", "priority", "channel_standardized") \
    .agg(
        count("ticket_id").alias("total_tickets"),
        sum(when(col("is_resolved"), 1).otherwise(0)).alias("resolved_tickets"),
        sum(when(col("is_open"), 1).otherwise(0)).alias("open_tickets"),
        avg("resolution_time_days").alias("avg_resolution_days"),
        min("resolution_time_days").alias("min_resolution_days"),
        max("resolution_time_days").alias("max_resolution_days")
    ) \
    .withColumn("resolution_rate", round(col("resolved_tickets") / col("total_tickets") * 100, 2)) \
    .orderBy(col("total_tickets").desc())

print(f"  Gold records: {df_support_metrics.count()} support categories")
display(df_support_metrics.limit(10))

gold_support_path = f"{gold_base_path}customer_support_metrics/"
df_support_metrics.write.format("delta").mode("overwrite").save(gold_support_path)
print(f"  ✓ Saved to: {gold_support_path}")

# ============================================
# 7. MONTHLY FINANCIAL SUMMARY (Gold)
# ============================================
print("\n2.7 Creating Monthly Financial Summary...")

df_monthly_financial = df_transactions_silver \
    .withColumn("transaction_year", year(col("transaction_date"))) \
    .withColumn("transaction_month", month(col("transaction_date"))) \
    .groupBy("transaction_year", "transaction_month") \
    .agg(
        count("transaction_id").alias("total_transactions"),
        sum(when(col("transaction_type") == "Credit", col("amount")).otherwise(0)).alias("total_credits"),
        sum(when(col("transaction_type") == "Debit", col("amount")).otherwise(0)).alias("total_debits"),
        sum(when(col("payment_mode_standardized") == "UPI", col("amount")).otherwise(0)).alias("upi_volume"),
        sum(when(col("payment_mode_standardized") == "Net Banking", col("amount")).otherwise(0)).alias("netbanking_volume"),
        sum(when(col("payment_mode_standardized") == "RTGS", col("amount")).otherwise(0)).alias("rtgs_volume"),
        sum(when(col("payment_mode_standardized") == "IMPS", col("amount")).otherwise(0)).alias("imps_volume")
    ) \
    .withColumn("net_flow", col("total_credits") - col("total_debits")) \
    .orderBy("transaction_year", "transaction_month")

print(f"  Gold records: {df_monthly_financial.count()} monthly records")
display(df_monthly_financial.limit(12))

gold_monthly_path = f"{gold_base_path}monthly_financial_summary/"
df_monthly_financial.write.format("delta").mode("overwrite").save(gold_monthly_path)
print(f"  ✓ Saved to: {gold_monthly_path}")

# ============================================
# 8. EMPLOYEE PERFORMANCE METRICS (Gold)
# ============================================
print("\n2.8 Creating Employee Performance Metrics...")

# FIX: 'experience_level' column does not exist in employees silver.
# Derived here from tenure_years instead.
df_employee_performance = df_employees_silver \
    .join(df_branches_silver.select("branch_id", "branch_name_clean", "city_clean"), "branch_id", "left") \
    .withColumn("experience_level",
        when(col("tenure_years") < 2, "Junior (<2 yrs)")
        .when(col("tenure_years") < 5, "Mid (2-5 yrs)")
        .when(col("tenure_years") < 10, "Senior (5-10 yrs)")
        .otherwise("Expert (>10 yrs)")) \
    .groupBy("branch_name_clean", "designation_clean", "experience_level") \
    .agg(
        count("employee_id").alias("employee_count"),
        avg("salary").alias("avg_salary"),
        sum(when(col("is_active_employee"), 1).otherwise(0)).alias("active_employees")
    ) \
    .orderBy(col("avg_salary").desc())

print(f"  Gold records: {df_employee_performance.count()} employee segments")
display(df_employee_performance.limit(10))

gold_employee_path = f"{gold_base_path}employee_performance/"
df_employee_performance.write.format("delta").mode("overwrite").save(gold_employee_path)
print(f"  ✓ Saved to: {gold_employee_path}")

# ============================================
# 9. PRODUCT CROSS-SELL INSIGHTS (Gold)
# ============================================
print("\n2.9 Creating Product Cross-Sell Insights...")

# FIX: Pre-aggregate account counts per customer before joining,
# so total_accounts is available as a proper column in the joined df.
df_accounts_per_customer = df_accounts_silver.groupBy("customer_id").agg(
    count("account_id").alias("total_accounts"),
    collect_set("account_type").alias("account_types")
)

df_loans_per_customer = df_loans_silver.groupBy("customer_id").agg(
    count("loan_id").alias("has_loan")
)

df_cards_per_customer = df_cards_silver.groupBy("customer_id").agg(
    count("card_id").alias("has_card")
)

df_insurance_per_customer = df_insurance_silver.groupBy("customer_id").agg(
    count("policy_id").alias("has_insurance")
)

df_cross_sell = df_customers_silver \
    .join(df_accounts_per_customer, "customer_id", "left") \
    .join(df_loans_per_customer, "customer_id", "left") \
    .join(df_cards_per_customer, "customer_id", "left") \
    .join(df_insurance_per_customer, "customer_id", "left") \
    .fillna(0) \
    .withColumn("product_count",
        col("total_accounts") + col("has_loan") + col("has_card") + col("has_insurance")) \
    .groupBy("age_group", "income_bracket", "city") \
    .agg(
        count("customer_id").alias("customer_count"),
        avg("product_count").alias("avg_products_per_customer"),
        sum(when(col("has_loan") > 0, 1).otherwise(0)).alias("loan_customers"),
        sum(when(col("has_card") > 0, 1).otherwise(0)).alias("card_customers"),
        sum(when(col("has_insurance") > 0, 1).otherwise(0)).alias("insurance_customers")
    ) \
    .withColumn("loan_penetration", round(col("loan_customers") / col("customer_count") * 100, 2)) \
    .withColumn("card_penetration", round(col("card_customers") / col("customer_count") * 100, 2)) \
    .withColumn("insurance_penetration", round(col("insurance_customers") / col("customer_count") * 100, 2))

print(f"  Gold records: {df_cross_sell.count()} customer segments")
display(df_cross_sell.limit(10))

gold_cross_sell_path = f"{gold_base_path}cross_sell_insights/"
df_cross_sell.write.format("delta").mode("overwrite").save(gold_cross_sell_path)
print(f"  ✓ Saved to: {gold_cross_sell_path}")

# ============================================
# 10. ACCOUNT BALANCE DISTRIBUTION (Gold)
# ============================================
print("\n2.10 Creating Account Balance Distribution...")

df_balance_distribution = df_accounts_silver \
    .join(df_branches_silver.select("branch_id", "city_clean", "state_clean"), "branch_id", "left") \
    .groupBy("account_type", "balance_category", "city_clean", "state_clean") \
    .agg(
        count("account_id").alias("account_count"),
        sum("balance").alias("total_balance"),
        avg("balance").alias("avg_balance"),
        sum(when(col("is_active"), 1).otherwise(0)).alias("active_accounts")
    ) \
    .orderBy(col("total_balance").desc())

print(f"  Gold records: {df_balance_distribution.count()} balance segments")
display(df_balance_distribution.limit(10))

gold_balance_path = f"{gold_base_path}balance_distribution/"
df_balance_distribution.write.format("delta").mode("overwrite").save(gold_balance_path)
print(f"  ✓ Saved to: {gold_balance_path}")

print("\n" + "="*70)
print("✅ GOLD LAYER COMPLETED - All business tables saved to ADLS")
print("="*70)