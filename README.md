# 🏦 Azure End-to-End Banking Data Pipeline

![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?style=for-the-badge&logo=microsoft-azure)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-FF3621?style=for-the-badge&logo=databricks)
![Synapse](https://img.shields.io/badge/Synapse-Analytics-0078D4?style=for-the-badge&logo=microsoft-azure)
![Status](https://img.shields.io/badge/Status-Completed-brightgreen?style=for-the-badge)

A **production-grade, end-to-end data engineering pipeline** built on Microsoft Azure, implementing the **Medallion Architecture (Bronze → Silver → Gold)** for a banking domain use case. The pipeline ingests on-premises SQL Server data, transforms it through multiple layers using PySpark on Databricks, and exposes business-ready data via Synapse Analytics Serverless SQL.

---

## 📐 Architecture Overview

![Architecture Diagram](architecture/architecture_diagram.png)

> **How to read this diagram:** Data flows top-to-bottom — from the on-premises SQL Server source, through ADF ingestion, across the three Medallion layers in ADLS Gen2, through Databricks transformations, into Synapse Analytics for SQL serving, and finally out to any BI consumption tool.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        DATA SOURCE LAYER                                │
│                                                                         │
│   ┌──────────────────────────────────────────────────┐                  │
│   │         On-Premises SQL Server (SSMS)            │                  │
│   │         Database: Employeedb                     │                  │
│   │         10 Tables: Customers, Accounts,          │                  │
│   │         Transactions, Loans, Credit Cards,       │                  │
│   │         Branches, Employees, Fraud,              │                  │
│   │         Insurance, Support Tickets               │                  │
│   └──────────────────┬───────────────────────────────┘                  │
└──────────────────────┼──────────────────────────────────────────────────┘
                       │  Self-Hosted Integration Runtime (SHIR)
                       │  (Bridge between On-Prem & Azure Cloud)
                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                       INGESTION LAYER                                   │
│                                                                         │
│   ┌──────────────────────────────────────────────────┐                  │
│   │         Azure Data Factory (ADF)                 │                  │
│   │         Pipeline: pipeline1                      │                  │
│   │         Activity: Copy Data                      │                  │
│   │         Linked Service: SQL Server → ADLS Gen2   │                  │
│   │         Secrets managed via Azure Key Vault      │                  │
│   └──────────────────┬───────────────────────────────┘                  │
└──────────────────────┼──────────────────────────────────────────────────┘
                       │  Raw data copied as Parquet/Delta
                       ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    STORAGE LAYER — ADLS Gen2                            │
│                    Storage Account: bankingdatalake789                  │
│                                                                         │
│   ┌─────────────┐    ┌─────────────┐    ┌─────────────┐                │
│   │   BRONZE    │    │   SILVER    │    │    GOLD     │                │
│   │  Container  │    │  Container  │    │  Container  │                │
│   │             │    │             │    │             │                │
│   │ Raw data    │    │ Cleaned &   │    │ Aggregated  │                │
│   │ as-is from  │───▶│ enriched    │───▶│ business    │                │
│   │ source      │    │ data        │    │ metrics     │                │
│   │             │    │             │    │             │                │
│   │ 10 tables   │    │ 10 tables   │    │ 10 views    │                │
│   └─────────────┘    └─────────────┘    └─────────────┘                │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │  PySpark Notebooks
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   TRANSFORMATION LAYER                                  │
│                                                                         │
│   ┌──────────────────────────────────────────────────┐                  │
│   │         Azure Databricks                         │                  │
│   │                                                  │                  │
│   │  Notebook 1: Bronze Ingestion                    │                  │
│   │  → Reads from ADF-loaded ADLS Bronze             │                  │
│   │  → Applies schema, adds audit columns            │                  │
│   │  → Saves as Delta format                         │                  │
│   │                                                  │                  │
│   │  Notebook 2: Silver Transformations              │                  │
│   │  → Deduplication & null handling                 │                  │
│   │  → Type casting (STRING → DECIMAL/INT)           │                  │
│   │  → Feature engineering (age groups,              │                  │
│   │    income brackets, risk categories)             │                  │
│   │  → Standardization of categorical columns        │                  │
│   │                                                  │                  │
│   │  Notebook 3: Gold Aggregations                   │                  │
│   │  → Customer 360 view                             │                  │
│   │  → Branch performance metrics                    │                  │
│   │  → Loan portfolio analysis                       │                  │
│   │  → Fraud analysis dashboard                      │                  │
│   │  → Monthly financial summary                     │                  │
│   │  → Cross-sell insights                           │                  │
│   └──────────────────────────────────────────────────┘                  │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │  Gold Delta tables
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   SERVING LAYER                                         │
│                                                                         │
│   ┌──────────────────────────────────────────────────┐                  │
│   │         Azure Synapse Analytics                  │                  │
│   │         Workspace: synapse-banking-prod          │                  │
│   │                                                  │                  │
│   │  Serverless SQL Pool                             │                  │
│   │  Database: banking_analytics_db                  │                  │
│   │                                                  │                  │
│   │  Views created on Gold layer:                    │                  │
│   │  → vw_customer_360                               │                  │
│   │  → vw_branch_performance                         │                  │
│   │  → vw_fraud_analysis                             │                  │
│   │  → vw_monthly_financial                          │                  │
│   └──────────────────────────────────────────────────┘                  │
└───────────────────────────┬─────────────────────────────────────────────┘
                            │  SQL Endpoint / JDBC
                            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   CONSUMPTION LAYER                                     │
│                                                                         │
│         Power BI / Tableau / SQL Clients                                │
│         (Dashboard development in progress)                             │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Source | SQL Server (On-Premises) | Source OLTP database |
| Connectivity | Self-Hosted Integration Runtime | Bridge on-prem to cloud |
| Orchestration | Azure Data Factory (ADF) | Pipeline orchestration & data ingestion |
| Storage | Azure Data Lake Storage Gen2 | Multi-layer data storage (Bronze/Silver/Gold) |
| Transformation | Azure Databricks (PySpark) | Large-scale data transformation |
| File Format | Delta Lake | ACID-compliant storage format |
| Security | Azure Key Vault | Credential & secret management |
| Serving | Azure Synapse Analytics | Serverless SQL query engine |
| Language | Python (PySpark), SQL (T-SQL) | Transformation & querying |

---

## 📂 Repository Structure

```
azure-banking-datalake-pipeline/
│
├── README.md                          ← You are here
│
├── architecture/
│   └── architecture_diagram.png       ← Architecture visual
│
├── databricks/
│   ├── 01_bronze_ingestion.py         ← Bronze layer notebook
│   ├── 03_silver_transformations.py   ← Silver layer notebook
│   └── 04_gold_transformations.py     ← Gold layer notebook
│
├── synapse/
│   └── create_views.sql               ← Serverless SQL view definitions
│
├── adf/
│   └── pipeline_screenshots/          ← ADF pipeline screenshots
│
└── docs/
    └── data_dictionary.md             ← Column descriptions per table
```

---

## 📊 Dataset Overview

The pipeline processes **10 banking domain tables** covering end-to-end retail banking operations:

| Table | Records (Approx.) | Description |
|---|---|---|
| `customers` | 10,000+ | Customer demographics, KYC, income |
| `accounts` | 15,000+ | Bank accounts, balances, types |
| `transactions` | 100,000+ | Debit/credit transaction history |
| `loans` | 8,000+ | Loan disbursements, EMI, status |
| `credit_cards` | 7,000+ | Card limits, utilization, status |
| `branches` | 50+ | Branch locations, managers |
| `employees` | 500+ | Staff details, designations, salary |
| `fraud_transactions` | 2,000+ | Fraud cases, risk scores, losses |
| `insurance_products` | 5,000+ | Policies, premiums, coverage |
| `customer_support_tickets` | 20,000+ | Issue types, resolution times |

---

## 🔄 Data Flow — Medallion Architecture

### 🥉 Bronze Layer — Raw Ingestion
- Data copied **as-is** from on-premises SQL Server via ADF
- Stored in Delta format in ADLS Gen2 `bronze` container
- Audit columns added: `ingestion_timestamp`, `source_file`, `ingestion_batch_id`
- **No transformations applied** — preserves source fidelity

### 🥈 Silver Layer — Cleansed & Enriched
Key transformations applied per table:
- **Deduplication** on primary keys
- **Null filtering** on critical columns
- **Type casting** — all monetary columns cast to `DECIMAL(18,2)` for precision
- **Feature engineering:**
  - Age groups (Young / Adult / Middle Age / Senior)
  - Income brackets (Low / Medium / High / Very High)
  - Balance categories, loan amount tiers, credit utilization bands
- **Standardization** of categorical columns (gender, status, payment modes)
- **Date features** extracted (year, month, quarter, day name)
- Silver batch ID and processing timestamp added

### 🥇 Gold Layer — Business Aggregations
10 aggregated business views created:

| Gold Table | Business Use Case |
|---|---|
| `customer_360` | Unified customer profile with all product holdings |
| `branch_performance` | Branch-wise deposits, loans, transactions, efficiency |
| `loan_portfolio` | Loan segmentation by type, status, income bracket |
| `fraud_analysis` | Monthly fraud trends, confirmation rates, losses |
| `daily_transaction_trends` | Daily transaction volumes, success rates by mode |
| `customer_support_metrics` | Resolution rates, SLA tracking by issue type |
| `monthly_financial_summary` | Monthly credit/debit flows by payment mode |
| `employee_performance` | Headcount, salary bands by branch and designation |
| `cross_sell_insights` | Product penetration rates by customer segment |
| `balance_distribution` | Account balance spread by type, city, state |

---

## 🔐 Security Implementation

- All credentials (SQL Server password, storage account key) stored in **Azure Key Vault**
- ADF linked services reference Key Vault secrets — **no hardcoded credentials**
- Synapse workspace uses **Managed Identity** to access ADLS Gen2
- ADLS Gen2 access controlled via **RBAC (Storage Blob Data Contributor)**

---

## ⚙️ Key Implementation Details

### Self-Hosted Integration Runtime
Since the source SQL Server is **on-premises**, a Self-Hosted Integration Runtime (SHIR) was installed on the local machine to establish a secure tunnel between the on-prem network and Azure cloud — without exposing the database publicly.

### Delta Lake Format
All layers use **Delta Lake** as the storage format, providing:
- ACID transactions
- Schema enforcement
- Time travel (data versioning)
- Efficient upserts

### Type Safety in PySpark
All Bronze-to-Silver transformations explicitly cast string columns to appropriate numeric types **before** applying business logic — preventing `BIGINT cast` errors common when reading schema-less Delta files.

---

## 🚀 How to Reproduce This Project

### Prerequisites
- Azure subscription (Free tier works for learning)
- SQL Server Express installed locally (SSMS)
- Azure resources: ADLS Gen2, ADF, Databricks, Synapse, Key Vault

### Step-by-Step Setup
1. **Set up SQL Server** — Create database, load the 10 source tables
2. **Create ADLS Gen2** — Create `bronze`, `silver`, `gold` containers
3. **Set up Azure Key Vault** — Store SQL Server credentials as secrets
4. **Configure ADF** — Create linked services, datasets and copy pipeline with SHIR
5. **Run Bronze notebooks** in Databricks — Ingest data from ADLS bronze
6. **Run Silver notebooks** — Apply transformations and save to silver container
7. **Run Gold notebooks** — Build aggregations and save to gold container
8. **Set up Synapse** — Create serverless SQL database and views on gold layer
9. **Query views** — Validate data using `SELECT TOP 10 * FROM vw_customer_360`

---

## 📁 Synapse SQL Views

Views are created in `banking_analytics_db` using Synapse Serverless SQL pool pointing to Gold layer Parquet files:

```sql
CREATE VIEW vw_customer_360
AS
SELECT *
FROM OPENROWSET(
    BULK 'https://<your-storage-account>.dfs.core.windows.net/gold/customer_360/*.parquet',
    FORMAT = 'PARQUET'
) AS customer_data;
```
> See full SQL scripts in `/synapse/create_views.sql`

---

## 🌱 Future Enhancements

- [ ] Power BI dashboard connected to Synapse SQL endpoint
- [ ] ADF pipeline scheduling (daily incremental loads)
- [ ] Data quality checks using Great Expectations or Databricks DLT
- [ ] CI/CD pipeline using Azure DevOps
- [ ] Alerts and monitoring via Azure Monitor

---

## 👤 Author

**Mayank**
Data Engineer | Azure | PySpark | Power BI
📍 Delhi, India

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?style=flat&logo=linkedin)](https://linkedin.com/in/your-profile)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?style=flat&logo=github)](https://github.com/your-username)

---

## 📄 License

This project is licensed under the MIT License — feel free to use it as a reference for your own learning projects.
