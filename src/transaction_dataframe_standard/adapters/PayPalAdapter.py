"""
PayPal Adapter

Parses PayPal CSV transaction exports which include:
- Payments to merchants (negative amounts): Purchases, subscriptions
- Bank deposits (positive amounts): Auto-funding from linked bank account
- Refunds (positive amounts): Merchant refunds
- Currency conversions: FX transactions

Account Type: Payment Service
"""

import pandas as pd
import re
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    create_empty_standard_dataframe
)


class PayPalAdapter:
    """
    Adapter for PayPal CSV transaction exports.

    Extracts transaction data from PayPal CSV downloads including
    merchant payments, bank deposits (auto-funding), refunds, and currency conversions.
    """

    def __init__(self, csv_paths, account_name: str = "PayPal"):
        """
        Initialize PayPal adapter.

        Args:
            csv_paths: List of paths to PayPal CSV files (or single path)
            account_name: Name for the account (default: "PayPal")
        """
        # Handle single path or list of paths
        if isinstance(csv_paths, str):
            csv_paths = [csv_paths]

        self.csv_paths = [Path(p) for p in csv_paths]
        self.account_name = account_name

        # Parse all CSVs
        self._transactions = self._parse_all_csvs()

    def _parse_all_csvs(self) -> pd.DataFrame:
        """Parse all CSV files and combine into single DataFrame."""
        all_transactions = []

        print(f"Parsing {len(self.csv_paths)} PayPal CSV files...")

        for csv_path in sorted(self.csv_paths):
            try:
                transactions = self._parse_single_csv(csv_path)
                all_transactions.extend(transactions)
                print(f"  ✓ {csv_path.name}: {len(transactions)} transactions")
            except Exception as e:
                print(f"  ✗ Error parsing {csv_path.name}: {e}")
                import traceback
                traceback.print_exc()
                continue

        if not all_transactions:
            return create_empty_standard_dataframe()

        df = pd.DataFrame(all_transactions)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        print(f"  ✓ Total: {len(df)} PayPal transactions")
        return df

    def _parse_single_csv(self, csv_path: Path) -> List[dict]:
        """Parse a single PayPal CSV file."""
        transactions = []

        # Read CSV with UTF-8 BOM encoding
        df = pd.read_csv(csv_path, encoding='utf-8-sig')

        for _, row in df.iterrows():
            txn = self._parse_transaction_row(row, csv_path)
            if txn:
                transactions.append(txn)

        return transactions

    def _normalize_payee(self, name: str, txn_type: str) -> str:
        """
        Normalize merchant/payee name.

        Applies common merchant name mappings for better readability.
        """
        # If no name (e.g., for bank deposits), return descriptive text
        if not name or pd.isna(name) or name.strip() == '':
            if 'Bank deposit' in txn_type:
                return 'PayPal Auto-funding'
            elif 'Withdrawal' in txn_type:
                return 'PayPal Withdrawal'
            elif 'Currency Conversion' in txn_type:
                return 'PayPal FX Conversion'
            else:
                return 'PayPal'

        # Common merchant mappings
        merchant_map = {
            # Healthcare
            'HARLEY STREET DENTAL': 'Harley Street Dental S',

            # Remove "UK LIMITED", "LTD", etc.
            'STITCH FIX UK LTD': 'Stitch Fix',
            'STITCH FIX UK LIMITED': 'Stitch Fix',
            'AIRBNB PAYMENTS UK LIMITED': 'Airbnb',
            'SAMSUNG ELECTRONICS (UK) LIMITED': 'Samsung',
            'PRIVATE INTERNET ACCESS, INC.': 'Private Internet Access',
            'BLINKS LABS GMBH': 'Blinks Labs',
            'SUPERPROF': 'Superprof',
        }

        name_upper = name.upper()
        for pattern, normalized in merchant_map.items():
            if pattern in name_upper:
                return normalized

        # Clean up common suffixes
        cleaned = name
        suffixes = [' UK LIMITED', ' UK LTD', ' LIMITED', ' LTD', ' INC.', ' INC', ' GMBH', ' PAYMENTS']
        for suffix in suffixes:
            if cleaned.upper().endswith(suffix):
                cleaned = cleaned[:len(cleaned)-len(suffix)]
                break

        # Title case for readability
        return cleaned.strip().title()

    def _parse_transaction_row(self, row: pd.Series, source_file: Path) -> Optional[dict]:
        """
        Parse a single transaction row from PayPal CSV.

        Row format:
        - Date: DD/MM/YYYY
        - Time: HH:MM:SS
        - Name: Merchant name (empty for bank deposits)
        - Type: Transaction type
        - Status: Completed, Pending, Removed, etc.
        - Currency: GBP, USD, EUR, etc.
        - Amount: Positive or negative
        - Total: Same as amount
        - Transaction ID: Unique ID
        """
        try:
            # Parse date and time
            date_str = str(row['Date']).strip()
            time_str = str(row['Time']).strip()
            txn_date = datetime.strptime(date_str, '%d/%m/%Y').date()
            txn_time = time_str

            # Extract transaction details
            name = str(row['Name']) if pd.notna(row['Name']) else ''
            txn_type = str(row['Type']).strip()
            status = str(row['Status']).strip()
            currency = str(row['Currency']).strip()
            amount = float(str(row['Amount']).replace(',', ''))
            transaction_id = str(row['Transaction ID']).strip()

            # Skip removed/cancelled transactions
            if status in ['Removed', 'Cancelled', 'Denied']:
                return None

            # Normalize payee
            payee = self._normalize_payee(name, txn_type)

            # Classify transaction
            txn_class, category = self._classify_transaction(txn_type, amount, name)

            # Build notes
            notes = f"Payee: {payee}"
            if name and name != payee:
                notes += f" | Merchant: {name}"
            notes += f" | Type: {txn_type}"
            if status != 'Completed':
                notes += f" | Status: {status}"
            notes += f" | Transaction ID: {transaction_id}"

            return {
                'date': txn_date,
                'time': txn_time,
                'account': self.account_name,
                'account_type': AccountType.CURRENT.value,  # PayPal is like a current account
                'transaction_type': txn_class,
                'category': category,
                'amount': amount,
                'currency': currency,
                'asset_ticker': None,
                'units': None,
                'price_per_unit': None,
                'notes': notes,
                'country': 'UK',
                'city': None,
                'is_pension_contribution': False,
                'data_source': source_file.name,
                'data_quality': DataQuality.VERIFIED.value
            }

        except Exception as e:
            print(f"  ⚠ Error parsing row: {e}")
            return None

    def _classify_transaction(self, txn_type: str, amount: float, name: str) -> Tuple[str, str]:
        """
        Classify PayPal transaction type and category.

        Rules:
        - Bank deposits (positive): Initially Expense, will be reclassified to Transfer if matched with Monzo
        - Merchant payments (negative): Expense
        - Refunds (positive): Expense (negative expense)
        - Currency conversions: Skip or FX
        - Withdrawals: Transfer
        """
        txn_type_lower = txn_type.lower()

        # Bank deposits from linked account (auto-funding)
        # These should be matched with Monzo payments to PayPal
        if 'bank deposit' in txn_type_lower:
            # Initially classify as Expense - will be reclassified to Transfer if matched
            return (TransactionType.EXPENSE.value, 'PayPal Deposit from Bank')

        # Merchant payments
        elif 'payment' in txn_type_lower or 'checkout' in txn_type_lower:
            if amount < 0:
                # Payment to merchant
                if name:
                    return (TransactionType.EXPENSE.value, f'PayPal Payment')
                else:
                    return (TransactionType.EXPENSE.value, 'PayPal Payment')
            else:
                # Shouldn't happen, but handle it
                return (TransactionType.EXPENSE.value, 'PayPal Receipt')

        # Refunds
        elif 'refund' in txn_type_lower:
            return (TransactionType.EXPENSE.value, 'PayPal Refund')

        # Withdrawals
        elif 'withdrawal' in txn_type_lower:
            return (TransactionType.TRANSFER.value, 'PayPal Withdrawal')

        # Currency conversions (usually skip these as they're paired with actual transactions)
        elif 'currency conversion' in txn_type_lower:
            return (TransactionType.EXPENSE.value, 'PayPal FX Conversion')

        # Default: Expense
        else:
            return (TransactionType.EXPENSE.value, 'PayPal Transaction')

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the parsed transactions DataFrame."""
        return self._transactions.copy()
