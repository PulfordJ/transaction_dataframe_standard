"""
Monzo Transactions Adapter

Converts Monzo CSV exports to the standardized transaction format.
"""

import pandas as pd
from pathlib import Path
from typing import Optional
from ..standard import TransactionType


class MonzoTransactionsAdapter:
    """
    Adapter for Monzo CSV transaction exports.

    Converts Monzo transaction data to the standardized format.
    """

    def __init__(self, csv_path: str, account_name: str = "Monzo"):
        """
        Initialize the Monzo adapter with a CSV file path.

        Args:
            csv_path: Path to Monzo CSV export file
            account_name: Name to use for account (default: "Monzo")
        """
        self.csv_path = Path(csv_path)
        self.account_name = account_name

        # Parse the CSV
        self._transactions = self._parse_csv()

    def _parse_csv(self) -> pd.DataFrame:
        """Parse Monzo CSV and convert to standard format."""

        # Read CSV with dayfirst=True for UK date format
        df = pd.read_csv(self.csv_path, sep=',')

        # Strip whitespace from column names
        df.columns = df.columns.str.strip()

        # Combine Date and Time into datetime
        df['DateTime'] = pd.to_datetime(
            df['Date'] + ' ' + df['Time'],
            dayfirst=True,
            errors='coerce'
        )

        # Build standard transactions
        transactions = []

        for _, row in df.iterrows():
            # Skip rows with invalid dates
            if pd.isna(row['DateTime']):
                continue

            # Extract date and time
            date = row['DateTime'].date()
            time = row['DateTime'].time()

            # Get amount directly from CSV (already has correct sign)
            amount = float(row.get('Amount', 0) or 0)

            # Determine transaction type and category
            transaction_type, category = self._classify_transaction(
                monzo_type=str(row.get('Type', '')),
                monzo_category=str(row.get('Category', '')),
                amount=amount,
                name=str(row.get('Name', ''))
            )

            # Build notes from available fields
            notes_parts = []
            if row.get('Name'):
                notes_parts.append(f"Payee: {row['Name']}")
            if row.get('Description') and str(row['Description']) != 'nan':
                notes_parts.append(f"Description: {row['Description']}")
            if row.get('Notes and #tags') and str(row['Notes and #tags']) != 'nan':
                notes_parts.append(f"Notes: {row['Notes and #tags']}")

            notes = " | ".join(notes_parts) if notes_parts else None

            # Create standard transaction record
            transaction = {
                'date': date,
                'time': time,
                'account': self.account_name,
                'account_type': 'Current',  # Monzo is a current account
                'transaction_type': transaction_type,
                'category': category,
                'amount': amount,
                'currency': 'GBP',
                'asset_ticker': None,
                'units': None,
                'price_per_unit': None,
                'notes': notes,
                'country': 'UK',
                'city': None,
                'is_pension_contribution': False,
                'data_source': self.csv_path.name,
                'data_quality': 'Verified'
            }

            transactions.append(transaction)

        # Convert to DataFrame
        df_transactions = pd.DataFrame(transactions)

        # Set proper data types
        df_transactions['date'] = pd.to_datetime(df_transactions['date'])
        df_transactions['amount'] = df_transactions['amount'].astype(float)
        df_transactions['is_pension_contribution'] = df_transactions['is_pension_contribution'].astype(bool)

        return df_transactions

    def _classify_transaction(
        self,
        monzo_type: str,
        monzo_category: str,
        amount: float,
        name: str
    ) -> tuple[str, str]:
        """
        Classify a Monzo transaction into standard type and category.

        Args:
            monzo_type: Monzo transaction type
            monzo_category: Monzo category
            amount: Transaction amount (positive for income, negative for expenses)
            name: Payee/merchant name

        Returns:
            Tuple of (transaction_type, category)
        """
        # Normalize inputs
        monzo_type_lower = monzo_type.lower()
        monzo_category_lower = monzo_category.lower()
        name_lower = name.lower()

        # Handle transfers
        if 'transfer' in monzo_type_lower or 'pot transfer' in monzo_type_lower:
            return (TransactionType.TRANSFER.value, 'Account Transfer')

        # Handle income (positive amounts)
        if amount > 0:
            # Check for specific income types
            if 'salary' in name_lower or 'wages' in name_lower:
                return (TransactionType.INCOME.value, 'Salary')
            elif 'bonus' in name_lower:
                return (TransactionType.INCOME.value, 'Bonus')
            elif 'interest' in monzo_category_lower or 'interest' in name_lower:
                return (TransactionType.INTEREST.value, 'Interest Income')
            else:
                return (TransactionType.INCOME.value, 'Other Income')

        # Handle expenses (negative amounts)
        if amount < 0:
            # Map Monzo categories to our categories
            category_mapping = {
                'groceries': 'Groceries',
                'eating out': 'Eating Out',
                'entertainment': 'Entertainment',
                'transport': 'Transportation',
                'shopping': 'Shopping',
                'bills': 'Bills',
                'general': 'General',
                'expenses': 'General',
                'finances': 'Financial Services',
                'holidays': 'Travel',
                'family': 'Family',
                'personal care': 'Personal Care',
                'gifts': 'Gifts',
                'charity': 'Charity'
            }

            # Find matching category
            for key, value in category_mapping.items():
                if key in monzo_category_lower:
                    return (TransactionType.EXPENSE.value, value)

            # Default to General expense
            return (TransactionType.EXPENSE.value, 'General')

        # Default fallback
        return (TransactionType.EXPENSE.value, 'General')

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the processed transactions dataframe."""
        return self._transactions.copy()

    def get_summary(self) -> dict:
        """Get summary statistics about the transactions."""
        return {
            'total_transactions': len(self._transactions),
            'date_range': {
                'start': self._transactions['date'].min(),
                'end': self._transactions['date'].max()
            },
            'total_income': self._transactions[
                self._transactions['transaction_type'] == 'Income'
            ]['amount'].sum(),
            'total_expenses': self._transactions[
                self._transactions['transaction_type'] == 'Expense'
            ]['amount'].sum()
        }
