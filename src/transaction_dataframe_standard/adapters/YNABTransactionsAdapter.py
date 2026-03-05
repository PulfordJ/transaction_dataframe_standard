"""
YNAB (You Need A Budget) Transactions Adapter

Parses YNAB budget register CSV exports and extracts transaction data
into the comprehensive transaction standard format.

YNAB exports contain cash account transactions with detailed categorization.
"""

import pandas as pd
import re
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    STANDARD_COLUMNS, create_empty_standard_dataframe
)


class YNABTransactionsAdapter:
    """
    Adapter for YNAB budget register CSV exports.

    Extracts transaction data including:
    - Income
    - Expenses
    - Transfers between accounts
    - Starting balances
    """

    def __init__(self, csv_path: str):
        """
        Initialize the YNAB adapter with a CSV file path.

        Args:
            csv_path: Path to YNAB register CSV export
        """
        self.csv_path = Path(csv_path)

        # Parse the CSV
        self._transactions = self._parse_csv()

    def _parse_csv(self) -> pd.DataFrame:
        """Parse YNAB CSV and convert to comprehensive standard format."""
        print(f"Parsing YNAB file: {self.csv_path.name}")

        # Read CSV with proper encoding (YNAB exports often have BOM)
        df = pd.read_csv(
            self.csv_path,
            encoding='utf-8-sig',  # Handles BOM character
            parse_dates=['Date'],
            dayfirst=True
        )

        print(f"  Raw records: {len(df)}")

        transactions = []

        # Track transfers to avoid double-counting
        # (YNAB records both sides of a transfer)
        transfers_seen = set()

        for idx, row in df.iterrows():
            txn = self._parse_transaction_row(row, transfers_seen)
            if txn:
                transactions.append(txn)

        # Convert to DataFrame
        df_transactions = pd.DataFrame(transactions, columns=STANDARD_COLUMNS)

        # Set data types
        df_transactions['date'] = pd.to_datetime(df_transactions['date'], errors='coerce')
        df_transactions['amount'] = pd.to_numeric(df_transactions['amount'], errors='coerce')
        df_transactions['units'] = pd.to_numeric(df_transactions['units'], errors='coerce')
        df_transactions['price_per_unit'] = pd.to_numeric(df_transactions['price_per_unit'], errors='coerce')
        df_transactions['is_pension_contribution'] = df_transactions['is_pension_contribution'].astype(bool)

        print(f"  Transactions extracted: {len(df_transactions)}")

        return df_transactions

    def _parse_transaction_row(self, row, transfers_seen: set) -> Optional[List]:
        """
        Parse a single YNAB transaction row.

        YNAB columns:
        - Account: Account name
        - Date: Transaction date (DD/MM/YYYY)
        - Payee: Merchant/payee name
        - Category: Combined category (Master:Sub)
        - Master Category: Top-level category
        - Sub Category: Sub-category
        - Memo: Transaction notes
        - Outflow: Expense amount (with £)
        - Inflow: Income amount (with £)
        - Running Balance: Account balance after transaction

        Args:
            row: DataFrame row
            transfers_seen: Set to track transfer pairs

        Returns:
            Transaction row as list, or None if should be skipped
        """
        # Extract basic fields
        account = str(row['Account']).strip() if pd.notna(row['Account']) else "Unknown"
        date_val = row['Date']
        payee = str(row['Payee']).strip() if pd.notna(row['Payee']) else ""
        category = str(row['Category']).strip() if pd.notna(row['Category']) else ""
        master_category = str(row['Master Category']).strip() if pd.notna(row['Master Category']) else ""
        sub_category = str(row['Sub Category']).strip() if pd.notna(row['Sub Category']) else ""
        memo = str(row['Memo']).strip() if pd.notna(row['Memo']) else ""

        # Parse amounts (remove £ symbol and convert)
        outflow_str = str(row['Outflow']).strip() if pd.notna(row['Outflow']) else "£0.00"
        inflow_str = str(row['Inflow']).strip() if pd.notna(row['Inflow']) else "£0.00"

        try:
            outflow = float(outflow_str.replace('£', '').replace(',', ''))
        except:
            outflow = 0.0

        try:
            inflow = float(inflow_str.replace('£', '').replace(',', ''))
        except:
            inflow = 0.0

        # Calculate net amount (negative for outflows, positive for inflows)
        amount = inflow - outflow

        # Parse date
        try:
            if isinstance(date_val, (pd.Timestamp, datetime)):
                txn_date = pd.to_datetime(date_val)
            else:
                txn_date = pd.to_datetime(date_val, dayfirst=True)
        except:
            print(f"  Warning: Could not parse date: {date_val}")
            return None

        # Detect and handle transfers
        is_transfer = False
        if 'transfer' in payee.lower() or 'transfer' in category.lower():
            is_transfer = True

            # Create transfer ID to track both sides
            # Include payee to differentiate multiple transfers on same day with same amount
            transfer_id = f"{txn_date.date()}_{abs(amount):.2f}_{account}_{payee}"

            # Check if we've seen the other side of this transfer
            if transfer_id in transfers_seen:
                # Skip this transaction (already recorded the other side)
                return None
            else:
                # Record this side, skip the other when we see it
                transfers_seen.add(transfer_id)

        # Classify transaction type and category
        transaction_type, category_std = self._classify_transaction(
            payee, master_category, sub_category, amount, is_transfer
        )

        # Build notes from available fields
        notes_parts = []
        if payee:
            notes_parts.append(f"Payee: {payee}")
        if memo:
            notes_parts.append(memo)
        if category:
            notes_parts.append(f"YNAB Category: {category}")

        notes = " | ".join(notes_parts)

        # Determine account type (YNAB is primarily current accounts)
        account_type = AccountType.CURRENT.value

        # Build transaction row
        return [
            txn_date,                           # date
            None,                               # time
            f"YNAB - {account}",                # account (prefix with YNAB for clarity)
            account_type,                       # account_type
            transaction_type,                   # transaction_type
            category_std,                       # category
            amount,                             # amount
            'GBP',                              # currency
            None,                               # asset_ticker (YNAB is cash only)
            None,                               # units
            None,                               # price_per_unit
            notes,                              # notes
            'UK',                               # country (assume UK for YNAB data)
            None,                               # city
            False,                              # is_pension_contribution
            self.csv_path.name,                 # data_source
            DataQuality.VERIFIED.value,         # data_quality
        ]

    def _classify_transaction(
        self,
        payee: str,
        master_category: str,
        sub_category: str,
        amount: float,
        is_transfer: bool
    ) -> tuple[str, str]:
        """
        Classify YNAB transaction into standard transaction type and category.

        Args:
            payee: Payee name
            master_category: YNAB master category
            sub_category: YNAB sub category
            amount: Transaction amount
            is_transfer: Whether this is a transfer

        Returns:
            (transaction_type, category)
        """
        # Handle transfers
        if is_transfer:
            return (TransactionType.TRANSFER.value, 'Account Transfer')

        # Handle income
        if 'income' in master_category.lower() or amount > 0:
            # Classify income types
            if 'salary' in sub_category.lower() or 'salary' in payee.lower():
                return (TransactionType.INCOME.value, 'Salary')
            elif 'bonus' in sub_category.lower() or 'bonus' in payee.lower():
                return (TransactionType.INCOME.value, 'Bonus')
            elif 'interest' in sub_category.lower():
                return (TransactionType.INTEREST.value, 'Interest Income')
            elif 'starting balance' in payee.lower():
                return (TransactionType.INCOME.value, 'Starting Balance')
            else:
                return (TransactionType.INCOME.value, 'Other Income')

        # Handle expenses
        if amount < 0:
            # Use YNAB category as our category
            if master_category and sub_category:
                category_name = f"{master_category}: {sub_category}"
            elif master_category:
                category_name = master_category
            elif sub_category:
                category_name = sub_category
            else:
                category_name = 'Uncategorized'

            return (TransactionType.EXPENSE.value, category_name)

        # Default
        return (TransactionType.TRANSFER.value, 'Other')

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the processed transactions dataframe."""
        return self._transactions.copy()

    def get_summary(self) -> Dict:
        """Get a summary of parsed transactions."""
        income_total = self._transactions[
            self._transactions['transaction_type'] == TransactionType.INCOME.value
        ]['amount'].sum()

        expense_total = self._transactions[
            self._transactions['transaction_type'] == TransactionType.EXPENSE.value
        ]['amount'].sum()

        return {
            'total_transactions': len(self._transactions),
            'date_range': {
                'start': self._transactions['date'].min(),
                'end': self._transactions['date'].max()
            },
            'accounts': self._transactions['account'].unique().tolist(),
            'transaction_types': self._transactions['transaction_type'].value_counts().to_dict(),
            'total_income': income_total,
            'total_expenses': expense_total,
            'net_cashflow': income_total + expense_total,  # expense_total is negative
            'top_expense_categories': self._transactions[
                self._transactions['transaction_type'] == TransactionType.EXPENSE.value
            ]['category'].value_counts().head(10).to_dict()
        }
