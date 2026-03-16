# Finance Analytics - dbt + DuckDB Project

A personal finance analytics project built with **dbt Core** and **DuckDB**, demonstrating modern analytics engineering patterns using real-world financial data domains: budgeting, investment portfolios, and rental property accounting.

## Why This Project Exists

This project was built to gain hands-on experience with dbt's analytics engineering workflow -- the same tooling used at companies like Anthropic, Spotify, and GitLab. It uses DuckDB as a zero-cost, zero-config analytical database that runs entirely in-process.

## Architecture: Three-Layer Data Model

The project follows dbt's recommended staging -> intermediate -> marts pattern, which mirrors the classic acquisition -> conformed -> data mart architecture used in enterprise data warehousing:

```
seeds/               Raw CSV data (replaces an EL tool like Fivetran)
  |
models/staging/      1:1 cleaning of raw sources (views)
  |                  - Type casting, trimming, null filtering
  |                  - Derived fields (flow_direction, date parts)
  |
models/intermediate/ Business logic joins and transformations (views)
  |                  - Tax classification (IRS schedule mapping)
  |                  - Shared expense allocation across properties
  |                  - Portfolio weight and concentration analysis
  |
models/marts/        Final reporting tables (materialized as tables)
                     - Monthly budget summary with YTD running totals
                     - Portfolio performance by account and asset class
                     - Rental property P&L mapped to Schedule E line items
```

### Materialization Strategy

| Layer        | Materialization | Rationale                                      |
|-------------|----------------|-------------------------------------------------|
| Staging     | view           | Always reflects latest source; no storage cost  |
| Intermediate| view           | Transformation logic only; computed on read     |
| Marts       | table          | Pre-computed for fast dashboard queries          |

## Data Domains

### 1. Personal Budget (YNAB)
Transactions from checking and credit accounts, classified into budget categories. The intermediate layer adds tax classification so each transaction maps to the correct IRS schedule (W-2 income, Schedule C consulting, Schedule E rental, or personal).

### 2. Investment Portfolio (Schwab)
Portfolio holdings with cost basis, current prices, and derived metrics (unrealized gain/loss, portfolio weight, concentration flags). Summarized at both account level and asset class level in the mart.

### 3. Rental Properties (Schedule E)
Two rental properties owned through separate LLCs (Centaur and Minotaur). Transactions are attributed to properties via memo parsing; shared expenses like umbrella insurance are allocated proportionally by unit count. The mart output maps directly to IRS Schedule E line items.

## Testing and Documentation

Every model has a `schema.yml` file with:
- Column descriptions explaining business meaning
- `unique` and `not_null` tests on primary keys
- `relationships` tests validating foreign keys across models
- `accepted_values` tests enforcing domain constraints (account types, tax categories, etc.)

Current test suite: **50 tests, all passing.**

## Quick Start

```bash
# Install (Python 3.9+)
pip install dbt-core dbt-duckdb

# Configure profile (~/.dbt/profiles.yml)
# finance_analytics:
#   target: dev
#   outputs:
#     dev:
#       type: duckdb
#       path: finance_analytics.duckdb
#       threads: 4

# Run the pipeline
dbt seed              # Load CSV data into DuckDB
dbt run               # Build all models
dbt test              # Run 50 data quality tests
dbt docs generate     # Generate documentation site
dbt docs serve        # Browse interactive docs at localhost:8080
```

## Project Structure

```
finance_analytics/
├── dbt_project.yml              # Project configuration
├── seeds/                       # Raw data (CSV)
│   ├── raw_transactions.csv     # 25 financial transactions
│   ├── raw_accounts.csv         # 6 financial accounts
│   ├── raw_holdings.csv         # 8 portfolio positions
│   └── raw_properties.csv       # 2 rental properties
├── models/
│   ├── staging/                 # Layer 1: Clean and type-cast
│   │   ├── stg_transactions.sql
│   │   ├── stg_accounts.sql
│   │   ├── stg_holdings.sql
│   │   ├── stg_properties.sql
│   │   └── schema.yml
│   ├── intermediate/            # Layer 2: Business logic
│   │   ├── int_transactions_classified.sql
│   │   ├── int_rental_income_expenses.sql
│   │   ├── int_portfolio_enriched.sql
│   │   └── schema.yml
│   └── marts/                   # Layer 3: Reporting tables
│       ├── mart_monthly_budget_summary.sql
│       ├── mart_portfolio_performance.sql
│       ├── mart_rental_property_pnl.sql
│       └── schema.yml
└── README.md
```

## Key dbt Concepts Demonstrated

- **ref()** function for model dependencies and DAG construction
- **Materialization strategies** (view vs table) with layer-based configuration
- **Data tests** (unique, not_null, accepted_values, relationships)
- **Schema documentation** as code (YAML-based column descriptions)
- **Seed data** for bootstrapping development databases
- **CTE-based SQL style** following dbt best practices
- **Three-layer architecture** mirroring enterprise data warehouse patterns
- **Window functions** for YTD running totals
- **Cross-join allocation** for shared expense distribution

## Tech Stack

| Tool     | Purpose                          | Cost  |
|----------|----------------------------------|-------|
| dbt Core | SQL transformation framework     | Free  |
| DuckDB   | In-process analytical database   | Free  |
| Python   | dbt runtime environment          | Free  |
| Git      | Version control                  | Free  |
