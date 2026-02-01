"""
Vanguard Transactions Adapter

Parses Vanguard Excel transaction exports and extracts transaction data
into the comprehensive transaction standard format.

Vanguard exports contain multiple account sheets (ISA, General, Pension)
with cash and investment transactions.
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
from ..vanguard_fund_map import fund_name_to_ticker, extract_fund_name_from_details


class VanguardTransactionsAdapter:
    """
    Adapter for Vanguard Excel transaction exports.

    Extracts transaction data from multiple account sheets:
    - Cash deposits/withdrawals
    - Fund purchases/sales
    - Dividends
    - Interest
    - Fees
    - Inter-account transfers
    """

    def __init__(self, excel_path: str):
        """
        Initialize the Vanguard adapter with an Excel file path.

        Args:
            excel_path: Path to Vanguard LoadDocstore.Xlsx file
        """
        self.excel_path = Path(excel_path)
        self.excel_file = pd.ExcelFile(excel_path)

        # Parse all account sheets
        self._transactions = self._parse_all_sheets()

    def _parse_all_sheets(self) -> pd.DataFrame:
        """Parse all account sheets and combine into single dataframe."""
        all_transactions = []

        for sheet_name in self.excel_file.sheet_names:
            # Skip summary sheet
            if 'summary' in sheet_name.lower():
                continue

            print(f"Parsing sheet: {sheet_name}")

            # Determine account type from sheet name
            account_name, account_type = self._determine_account_type(sheet_name)

            transactions = self._parse_account_sheet(sheet_name, account_name, account_type)
            all_transactions.append(transactions)

        if all_transactions:
            combined = pd.concat(all_transactions, ignore_index=True)
            combined.sort_values('date', inplace=True)
            combined.reset_index(drop=True, inplace=True)
            return combined
        else:
            return create_empty_standard_dataframe()

    def _determine_account_type(self, sheet_name: str) -> tuple[str, str]:
        """
        Determine account name and type from sheet name.

        Args:
            sheet_name: Excel sheet name

        Returns:
            (account_name, account_type)
        """
        sheet_lower = sheet_name.lower()

        if 'isa' in sheet_lower:
            return ("Vanguard ISA", AccountType.ISA.value)
        elif 'pension' in sheet_lower:
            # Check if SIPP or regular pension
            if 'sipp' in sheet_lower:
                return ("Vanguard SIPP", AccountType.SIPP.value)
            else:
                return ("Vanguard Personal Pension", AccountType.PERSONAL_PENSION.value)
        elif 'housing' in sheet_lower or 'general' in sheet_lower:
            return ("Vanguard General", AccountType.GENERAL_INVESTMENT.value)
        else:
            # Default to general investment
            return (f"Vanguard {sheet_name}", AccountType.GENERAL_INVESTMENT.value)

    def _parse_account_sheet(self, sheet_name: str, account_name: str, account_type: str) -> pd.DataFrame:
        """
        Parse a single account sheet.

        Vanguard sheets have format:
        - Header rows (account name, "Cash Transactions", etc.)
        - Column headers: Date | Details | Amount | Balance
        - Transaction rows
        - Investment transactions section (if any)

        Args:
            sheet_name: Name of the Excel sheet
            account_name: Account name for the standard format
            account_type: Account type from enum

        Returns:
            DataFrame with transactions in comprehensive standard format
        """
        # Read the sheet
        df = pd.read_excel(self.excel_file, sheet_name=sheet_name, header=None)

        transactions = []

        # Find the "Date" column header row
        date_row_idx = None
        for idx, row in df.iterrows():
            if row[0] == 'Date' or (isinstance(row[0], str) and 'date' in str(row[0]).lower()):
                date_row_idx = idx
                break

        if date_row_idx is None:
            print(f"  Warning: Could not find Date column in {sheet_name}")
            return pd.DataFrame(columns=STANDARD_COLUMNS)

        # Column positions
        date_col = 0
        details_col = 1
        amount_col = 2
        balance_col = 3

        # Process data rows (starting after header)
        for idx in range(date_row_idx + 1, len(df)):
            row = df.iloc[idx]

            # Check if this is a valid transaction row
            if pd.isna(row[date_col]):
                continue

            # Skip non-date rows (section headers, etc.)
            date_val = row[date_col]
            if not self._is_valid_date(date_val):
                continue

            # Parse the transaction
            txn = self._parse_transaction_row(
                row,
                account_name,
                account_type,
                date_col,
                details_col,
                amount_col
            )

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

        print(f"  Found {len(df_transactions)} transactions")

        return df_transactions

    def _is_valid_date(self, value) -> bool:
        """Check if a value is a valid date."""
        if pd.isna(value):
            return False

        # If already a datetime
        if isinstance(value, (pd.Timestamp, datetime)):
            return True

        # Try to parse as date string
        if isinstance(value, str):
            # Check for date pattern DD/MM/YYYY
            if re.match(r'\d{1,2}/\d{1,2}/\d{4}', value):
                return True

        return False

    def _parse_transaction_row(
        self,
        row,
        account_name: str,
        account_type: str,
        date_col: int,
        details_col: int,
        amount_col: int
    ) -> Optional[List]:
        """
        Parse a single transaction row.

        Args:
            row: DataFrame row
            account_name: Account name
            account_type: Account type
            date_col: Column index for date
            details_col: Column index for details
            amount_col: Column index for amount

        Returns:
            Transaction row as list, or None if invalid
        """
        # Extract fields
        date_val = row[date_col]
        details = str(row[details_col]).strip() if not pd.isna(row[details_col]) else ""
        amount_str = str(row[amount_col]).strip() if not pd.isna(row[amount_col]) else "0"

        # Parse date
        try:
            if isinstance(date_val, (pd.Timestamp, datetime)):
                txn_date = pd.to_datetime(date_val)
            else:
                txn_date = pd.to_datetime(date_val, dayfirst=True)
        except:
            return None

        # Parse amount
        try:
            amount = float(str(amount_str).replace(',', ''))
        except:
            amount = 0.0

        # Classify transaction and extract investment details
        transaction_type, category, asset_ticker, units, price_per_unit = self._classify_and_extract(
            details, amount
        )

        # Check if this is a pension contribution
        is_pension_contribution = (
            account_type in [AccountType.SIPP.value, AccountType.PERSONAL_PENSION.value, AccountType.WORKPLACE_PENSION.value]
            and transaction_type == TransactionType.CONTRIBUTION.value
        )

        # Build transaction row
        return [
            txn_date,                           # date
            None,                               # time
            account_name,                       # account
            account_type,                       # account_type
            transaction_type,                   # transaction_type
            category,                           # category
            amount,                             # amount
            'GBP',                              # currency (Vanguard UK uses GBP)
            asset_ticker,                       # asset_ticker
            units,                              # units
            price_per_unit,                     # price_per_unit
            details,                            # notes
            'UK',                               # country
            None,                               # city
            is_pension_contribution,            # is_pension_contribution
            self.excel_path.name,               # data_source
            DataQuality.VERIFIED.value,         # data_quality (from official export)
        ]

    def _classify_and_extract(self, details: str, amount: float) -> tuple[str, str, Optional[str], Optional[float], Optional[float]]:
        """
        Classify transaction type and extract investment details.

        Args:
            details: Transaction details text
            amount: Transaction amount

        Returns:
            (transaction_type, category, asset_ticker, units, price_per_unit)
        """
        details_lower = details.lower()

        # Extract fund name, units, and OEIC status if this is an investment transaction
        fund_name, units, is_oeic = extract_fund_name_from_details(details)

        asset_ticker = None
        price_per_unit = None

        if fund_name:
            asset_ticker = fund_name_to_ticker(fund_name, is_oeic=is_oeic)

            # Calculate price per unit if we have units and amount
            if units and units != 0 and amount != 0:
                price_per_unit = abs(amount) / abs(units)

        # Classify transaction type
        if 'bought' in details_lower:
            return (
                TransactionType.BUY.value,
                'Fund Purchase',
                asset_ticker,
                units,
                price_per_unit
            )
        elif 'sold' in details_lower:
            return (
                TransactionType.SELL.value,
                'Fund Sale',
                asset_ticker,
                units,
                price_per_unit
            )
        elif 'div:' in details_lower or 'dividend' in details_lower:
            return (
                TransactionType.DIVIDEND.value,
                'Investment Income',
                asset_ticker,
                None,  # Dividends don't change unit holdings
                None
            )
        elif 'interest' in details_lower:
            return (
                TransactionType.INTEREST.value,
                'Interest Income',
                None,
                None,
                None
            )
        elif 'fee' in details_lower or 'charge' in details_lower:
            return (
                TransactionType.FEE.value,
                'Platform Fee',
                None,
                None,
                None
            )
        elif 'deposit' in details_lower:
            return (
                TransactionType.CONTRIBUTION.value,
                'Cash Deposit',
                None,
                None,
                None
            )
        elif 'withdrawal' in details_lower or 'payment' in details_lower:
            return (
                TransactionType.WITHDRAWAL.value,
                'Cash Withdrawal',
                None,
                None,
                None
            )
        elif 'transfer' in details_lower:
            # Distinguish between pension transfer in vs inter-account transfer
            if 'pension transfer in' in details_lower:
                return (
                    TransactionType.CONTRIBUTION.value,
                    'Pension Transfer In',
                    None,
                    None,
                    None
                )
            else:
                return (
                    TransactionType.TRANSFER.value,
                    'Account Transfer',
                    None,
                    None,
                    None
                )
        else:
            # Default to Transfer for unclassified
            return (
                TransactionType.TRANSFER.value,
                'Other',
                None,
                None,
                None
            )

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the processed transactions dataframe."""
        return self._transactions.copy()

    def get_summary(self) -> Dict:
        """Get a summary of parsed transactions."""
        return {
            'total_transactions': len(self._transactions),
            'date_range': {
                'start': self._transactions['date'].min(),
                'end': self._transactions['date'].max()
            },
            'accounts': self._transactions['account'].unique().tolist(),
            'transaction_types': self._transactions['transaction_type'].value_counts().to_dict(),
            'total_invested': self._transactions[self._transactions['transaction_type'] == TransactionType.BUY.value]['amount'].sum(),
            'total_withdrawn': self._transactions[self._transactions['transaction_type'] == TransactionType.SELL.value]['amount'].sum(),
            'unique_assets': self._transactions['asset_ticker'].dropna().unique().tolist()
        }
