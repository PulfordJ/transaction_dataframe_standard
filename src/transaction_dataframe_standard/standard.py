"""
Comprehensive Transaction Standard for Net Wealth Analysis

This module defines the standard schema for transaction data that supports both
cash transactions (bank accounts, credit cards) and investment transactions
(stocks, funds, ETFs, pensions).

The standard is designed to unify data from multiple sources:
- Cash accounts: Monzo, YNAB, Bank of America, etc.
- Investment platforms: Vanguard, Hargreaves Lansdown, Halifax, etc.
- Company accounts: Limited company transactions
- Pensions: SIPP, Personal Pension, Workplace Pension
"""

import pandas as pd
from enum import Enum
from typing import Optional


class TransactionType(Enum):
    """Standard transaction types across all account types"""
    INCOME = "Income"
    EXPENSE = "Expense"
    TRANSFER = "Transfer"
    BUY = "Buy"
    SELL = "Sell"
    DIVIDEND = "Dividend"
    INTEREST = "Interest"
    FEE = "Fee"
    CONTRIBUTION = "Contribution"
    WITHDRAWAL = "Withdrawal"
    LIQUIDATION = "Liquidation"


class AccountType(Enum):
    """Standard account type classifications"""
    CURRENT = "Current"                    # Current/checking account
    SAVINGS = "Savings"                    # Savings account
    CREDIT_CARD = "Credit Card"           # Credit card account
    ISA = "ISA"                           # Individual Savings Account (tax-advantaged)
    SIPP = "SIPP"                         # Self-Invested Personal Pension
    PERSONAL_PENSION = "Personal Pension" # Personal Pension (not SIPP)
    WORKPLACE_PENSION = "Workplace Pension" # Employer pension scheme
    GENERAL_INVESTMENT = "General Investment" # Taxable investment account
    LTD_COMPANY = "Ltd"                   # Limited company account
    HELP_TO_BUY = "Help to Buy ISA"      # Help to Buy ISA


class DataQuality(Enum):
    """Data quality indicators for tracking source reliability"""
    VERIFIED = "Verified"           # Direct from authoritative source (CSV/API export)
    ESTIMATED = "Estimated"         # Calculated or inferred from other data
    RECONSTRUCTED = "Reconstructed" # Manually reconstructed from statements/PDFs


# Standard column names
STANDARD_COLUMNS = [
    'date',                    # Date of transaction (datetime)
    'time',                    # Time of transaction (string, HH:MM:SS or None)
    'account',                 # Account name/identifier (e.g., "Monzo", "Vanguard ISA", "HL SIPP")
    'account_type',            # AccountType enum value
    'transaction_type',        # TransactionType enum value
    'category',                # Transaction category (e.g., "Groceries", "Salary", "Index Funds")
    'amount',                  # Transaction amount (positive or negative)
    'currency',                # Currency code (e.g., "GBP", "USD", "EUR")
    'asset_ticker',            # Asset ticker/symbol (e.g., "VWRL", "META", "BRK.B") - None for cash
    'units',                   # Number of shares/units (None for cash transactions)
    'price_per_unit',          # Price per share/unit in transaction currency (None for cash)
    'notes',                   # Additional notes/description
    'country',                 # Country where transaction occurred (for expense analysis)
    'city',                    # City where transaction occurred (for expense analysis)
    'is_pension_contribution', # Boolean - True if this is a pension contribution
    'data_source',             # Source file/system (e.g., "MonzoExport_2024.csv", "HL_Spring2024.pdf")
    'data_quality',            # DataQuality enum value
]


def create_empty_standard_dataframe() -> pd.DataFrame:
    """
    Create an empty DataFrame with the comprehensive standard schema.

    Returns:
        pd.DataFrame: Empty dataframe with correct column types
    """
    df = pd.DataFrame(columns=STANDARD_COLUMNS)

    # Set appropriate data types
    df['date'] = pd.to_datetime(df['date'])
    df['time'] = df['time'].astype('string')
    df['account'] = df['account'].astype('string')
    df['account_type'] = df['account_type'].astype('string')
    df['transaction_type'] = df['transaction_type'].astype('string')
    df['category'] = df['category'].astype('string')
    df['amount'] = pd.to_numeric(df['amount'])
    df['currency'] = df['currency'].astype('string')
    df['asset_ticker'] = df['asset_ticker'].astype('string')
    df['units'] = pd.to_numeric(df['units'])
    df['price_per_unit'] = pd.to_numeric(df['price_per_unit'])
    df['notes'] = df['notes'].astype('string')
    df['country'] = df['country'].astype('string')
    df['city'] = df['city'].astype('string')
    df['is_pension_contribution'] = df['is_pension_contribution'].astype('bool')
    df['data_source'] = df['data_source'].astype('string')
    df['data_quality'] = df['data_quality'].astype('string')

    return df


def validate_standard_dataframe(df: pd.DataFrame) -> tuple[bool, list[str]]:
    """
    Validate that a dataframe conforms to the comprehensive standard.

    Args:
        df: DataFrame to validate

    Returns:
        tuple: (is_valid, list_of_errors)
    """
    errors = []

    # Check all required columns exist
    missing_cols = set(STANDARD_COLUMNS) - set(df.columns)
    if missing_cols:
        errors.append(f"Missing required columns: {missing_cols}")

    # Check for invalid transaction_type values
    if 'transaction_type' in df.columns:
        valid_types = {t.value for t in TransactionType}
        invalid_types = set(df['transaction_type'].dropna().unique()) - valid_types
        if invalid_types:
            errors.append(f"Invalid transaction_type values: {invalid_types}")

    # Check for invalid account_type values
    if 'account_type' in df.columns:
        valid_account_types = {a.value for a in AccountType}
        invalid_account_types = set(df['account_type'].dropna().unique()) - valid_account_types
        if invalid_account_types:
            errors.append(f"Invalid account_type values: {invalid_account_types}")

    # Check for invalid data_quality values
    if 'data_quality' in df.columns:
        valid_quality = {q.value for q in DataQuality}
        invalid_quality = set(df['data_quality'].dropna().unique()) - valid_quality
        if invalid_quality:
            errors.append(f"Invalid data_quality values: {invalid_quality}")

    # Investment transactions should have asset_ticker and units
    if 'transaction_type' in df.columns:
        investment_types = {TransactionType.BUY.value, TransactionType.SELL.value}
        investment_txns = df[df['transaction_type'].isin(investment_types)]

        if len(investment_txns) > 0:
            missing_ticker = investment_txns['asset_ticker'].isna().sum()
            if missing_ticker > 0:
                errors.append(f"{missing_ticker} Buy/Sell transactions missing asset_ticker")

            missing_units = investment_txns['units'].isna().sum()
            if missing_units > 0:
                errors.append(f"{missing_units} Buy/Sell transactions missing units")

    return (len(errors) == 0, errors)


# Example usage and documentation
EXAMPLE_CASH_TRANSACTION = {
    'date': pd.Timestamp('2024-01-15'),
    'time': '14:23:45',
    'account': 'Monzo',
    'account_type': AccountType.CURRENT.value,
    'transaction_type': TransactionType.EXPENSE.value,
    'category': 'Groceries',
    'amount': -45.67,
    'currency': 'GBP',
    'asset_ticker': None,
    'units': None,
    'price_per_unit': None,
    'notes': 'Tesco - weekly shopping',
    'country': 'UK',
    'city': 'London',
    'is_pension_contribution': False,
    'data_source': 'Monzo_Export_2024.csv',
    'data_quality': DataQuality.VERIFIED.value,
}

EXAMPLE_INVESTMENT_TRANSACTION = {
    'date': pd.Timestamp('2024-01-15'),
    'time': None,
    'account': 'Vanguard ISA',
    'account_type': AccountType.ISA.value,
    'transaction_type': TransactionType.BUY.value,
    'category': 'Index Funds',
    'amount': -500.00,
    'currency': 'GBP',
    'asset_ticker': 'VWRL',
    'units': 6.234,
    'price_per_unit': 80.21,
    'notes': 'Regular monthly investment',
    'country': 'UK',
    'city': None,
    'is_pension_contribution': False,
    'data_source': 'Vanguard_LoadDocstore.xlsx',
    'data_quality': DataQuality.VERIFIED.value,
}

EXAMPLE_DIVIDEND_TRANSACTION = {
    'date': pd.Timestamp('2024-01-15'),
    'time': None,
    'account': 'HL General',
    'account_type': AccountType.GENERAL_INVESTMENT.value,
    'transaction_type': TransactionType.DIVIDEND.value,
    'category': 'Investment Income',
    'amount': 125.50,
    'currency': 'GBP',
    'asset_ticker': 'META',
    'units': None,  # Dividends don't change unit holdings
    'price_per_unit': None,
    'notes': 'Quarterly dividend payment',
    'country': 'UK',
    'city': None,
    'is_pension_contribution': False,
    'data_source': 'HL_Spring2024.pdf',
    'data_quality': DataQuality.RECONSTRUCTED.value,
}
