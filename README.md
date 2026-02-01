# Transaction DataFrame Standard

A Python library for standardizing transaction data from multiple financial sources into a unified format for net wealth analysis and expense tracking in Pandas.

## Overview

This library provides:
1. **Comprehensive Standard Schema** - A unified 17-column format supporting both cash and investment transactions
2. **Adapters** - Transform data from various sources (banks, investment platforms, budgeting apps) into the standard format
3. **Analysis Functions** - Process standardized data for insights (expense analysis, net wealth tracking, savings rates)

## Getting Started

The best way to get started is to take a look at the tests folder, which provide up to date examples on how to use this library.

## The Comprehensive Standard

The library uses a comprehensive standard that supports:
- **Cash transactions**: Bank accounts, credit cards, budgeting apps (Monzo, YNAB, Bank of America)
- **Investment transactions**: Stocks, funds, ETFs (Vanguard, Hargreaves Lansdown, Halifax)
- **Pensions**: SIPP, Personal Pension, Workplace Pension
- **Company accounts**: Limited company transactions

### Standard Columns (17 total)

| Column | Type | Description | Example |
|--------|------|-------------|---------|
| `date` | datetime | Transaction date | 2024-01-15 |
| `time` | string | Transaction time (optional) | 14:23:45 |
| `account` | string | Account identifier | "Monzo", "Vanguard ISA" |
| `account_type` | string | Account classification | "Current", "ISA", "SIPP" |
| `transaction_type` | string | Transaction category | "Expense", "Buy", "Dividend" |
| `category` | string | User-defined category | "Groceries", "Index Funds" |
| `amount` | float | Transaction amount | -45.67, 500.00 |
| `currency` | string | Currency code | "GBP", "USD" |
| `asset_ticker` | string | Investment ticker (optional) | "VWRL", "META", None |
| `units` | float | Shares/units (optional) | 6.234, None |
| `price_per_unit` | float | Price per share (optional) | 80.21, None |
| `notes` | string | Additional description | "Tesco - weekly shopping" |
| `country` | string | Transaction country (optional) | "UK", "Thailand" |
| `city` | string | Transaction city (optional) | "London", "Bangkok" |
| `is_pension_contribution` | bool | Pension contribution flag | True, False |
| `data_source` | string | Source file/system | "Monzo_Export_2024.csv" |
| `data_quality` | string | Data reliability | "Verified", "Estimated" |

### Standard Enums

**TransactionType**: Income, Expense, Transfer, Buy, Sell, Dividend, Interest, Fee, Contribution, Withdrawal, Liquidation

**AccountType**: Current, Savings, Credit Card, ISA, SIPP, Personal Pension, Workplace Pension, General Investment, Ltd, Help to Buy ISA

**DataQuality**: Verified, Estimated, Reconstructed

See `src/transaction_dataframe_standard/standard.py` for full documentation and examples.

## Adapters

Adapters transform source data into the standard format:

### Available Adapters

- **MonzoTransactionsAdapter** - Monzo bank CSV exports
- **BOATransactionsAdapter** - Bank of America credit card statements (partial)
- **HLTransactionsAdapter** - Hargreaves Lansdown quarterly PDFs (in development)
- **VanguardTransactionsAdapter** - Vanguard Excel exports (in development)
- **YNABTransactionsAdapter** - YNAB budget register CSV (in development)

### Example Usage

```python
from transaction_dataframe_standard.adapters import MonzoTransactionsAdapter
from transaction_dataframe_standard.standard import validate_standard_dataframe

# Load and transform Monzo data
adapter = MonzoTransactionsAdapter('path/to/monzo_export.csv')
df = adapter.transactions

# Validate the output
is_valid, errors = validate_standard_dataframe(df)
if not is_valid:
    print(f"Validation errors: {errors}")
```

## Analysis Functions

Process standardized data for insights. See `src/transaction_dataframe_standard/functions/` for available functions.

### Example: Expense Analysis

```python
from transaction_dataframe_standard.functions import print_expense_stats_trailing_twelve_months

# Analyze expenses over rolling 12-month periods
print_expense_stats_trailing_twelve_months(
    df,
    exclude_names=['Vanguard', 'HL']  # Exclude investments
)
```

## Legacy Format

The original 8-column format for cash-only transactions is still supported in existing Monzo/BOA adapters:
`Date, Time, Type, Name, Category, Description, Money Out, Money In`

New adapters should use the comprehensive 17-column standard.
