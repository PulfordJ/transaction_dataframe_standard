"""
Halifax Share Dealing Transactions Adapter

Parses Halifax Share Dealing PDF transaction history and extracts transaction data
into the comprehensive transaction standard format.

Halifax provides dealing history in PDF format with buy/sell transactions.
"""

import pandas as pd
import pdfplumber
import re
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    STANDARD_COLUMNS, create_empty_standard_dataframe
)
from ..halifax_ticker_map import halifax_ticker_to_standard


class HalifaxTransactionsAdapter:
    """
    Adapter for Halifax Share Dealing PDF transaction history.

    Extracts transaction data from dealing history PDFs including:
    - Stock/fund purchases
    - Stock/fund sales
    - Execution prices and quantities
    """

    def __init__(self, pdf_path: str, account_name: str = "Halifax Share Dealing", account_type: str = AccountType.GENERAL_INVESTMENT.value):
        """
        Initialize the Halifax adapter with a PDF file path.

        Args:
            pdf_path: Path to Halifax dealing history PDF
            account_name: Name to use for the account
            account_type: AccountType enum value
        """
        self.pdf_path = Path(pdf_path)
        self.account_name = account_name
        self.account_type = account_type

        # Parse the PDF
        self._transactions = self._parse_pdf()

    def _parse_pdf(self) -> pd.DataFrame:
        """Parse Halifax PDF dealing history."""
        print(f"Parsing Halifax PDF: {self.pdf_path.name}")

        transactions = []

        with pdfplumber.open(self.pdf_path) as pdf:
            for page_num, page in enumerate(pdf.pages):
                # Extract text (table detection doesn't work well for this PDF)
                text = page.extract_text()

                # Parse transactions from text
                # Pattern: Date (DD MMM YYYY on separate lines) Type Ticker Market Quantity Price Amount Reference
                # Example: "28 Jan\n2026\nSELL VRXXC FUND 36 20,980.00 7,633.79 21W2S38"

                lines = text.split('\n')

                i = 0
                while i < len(lines):
                    line = lines[i].strip()

                    # Look for transaction type (BUY or SELL)
                    if 'BUY' in line or 'SELL' in line:
                        # Pattern is:
                        # Line i-1: Day and Month (e.g., "28 Jan")
                        # Line i: Transaction details (BUY/SELL ...)
                        # Line i+1: Year (e.g., "2026")
                        if i >= 1 and i < len(lines) - 1:
                            date_part1 = lines[i-1].strip()  # "28 Jan"
                            date_part2 = lines[i+1].strip()  # "2026"

                            # Try to parse this as a transaction
                            txn = self._parse_transaction_from_text(date_part1, date_part2, line)
                            if txn:
                                transactions.append(txn)

                    i += 1

        # Convert to DataFrame
        df = pd.DataFrame(transactions, columns=STANDARD_COLUMNS)

        # Set data types
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['units'] = pd.to_numeric(df['units'], errors='coerce')
        df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
        df['is_pension_contribution'] = df['is_pension_contribution'].astype(bool)

        print(f"  Found {len(df)} transactions")

        return df

    def _parse_transaction_from_text(self, date_part1: str, date_part2: str, transaction_line: str) -> Optional[List]:
        """
        Parse a transaction from Halifax PDF text.

        Args:
            date_part1: First part of date (e.g., "28 Jan")
            date_part2: Second part of date (e.g., "2026")
            transaction_line: Transaction details (e.g., "SELL VRXXC FUND 36 20,980.00 7,633.79 21W2S38")

        Returns:
            Transaction row as list, or None if invalid
        """
        # Build full date string
        date_str = f"{date_part1} {date_part2}"

        # Parse date
        try:
            txn_date = pd.to_datetime(date_str, format='%d %b %Y')
        except:
            return None

        # Parse transaction line using regex
        # Pattern: TYPE TICKER MARKET QUANTITY PRICE AMOUNT REFERENCE
        # Example: "SELL VRXXC FUND 36 20,980.00 7,633.79 21W2S38"

        # Split the line into components
        parts = transaction_line.split()

        if len(parts) < 7:
            return None

        trade_type = parts[0]  # BUY or SELL
        ticker = parts[1]      # VRXXC, MDAABG
        market = parts[2]      # FUND
        quantity = parts[3]    # 36, 117,692
        price_pence = parts[4] # 20,980.00
        amount_gbp = parts[5]  # 7,633.79
        reference = parts[6] if len(parts) > 6 else ""  # 21W2S38

        # Parse transaction type
        if 'BUY' in trade_type.upper():
            transaction_type = TransactionType.BUY.value
            category = 'Fund Purchase' if 'FUND' in market else 'Stock Purchase'
        elif 'SELL' in trade_type.upper():
            transaction_type = TransactionType.SELL.value
            category = 'Fund Sale' if 'FUND' in market else 'Stock Sale'
        else:
            return None

        # Parse quantity
        try:
            units = float(quantity.replace(',', ''))
        except:
            units = None

        # Parse price (in pence, convert to pounds)
        try:
            price_pence_val = float(price_pence.replace(',', ''))
            price_per_unit = price_pence_val / 100  # Convert pence to pounds
        except:
            price_per_unit = None

        # Parse net consideration (already in pounds)
        try:
            amount = float(amount_gbp.replace(',', ''))
            # Make buys negative (cash outflow), sells positive (cash inflow)
            if transaction_type == TransactionType.BUY.value:
                amount = -abs(amount)
            else:
                amount = abs(amount)
        except:
            amount = 0.0

        # Map Halifax ticker to standard ticker
        asset_ticker = halifax_ticker_to_standard(ticker)

        # Build notes
        notes = f"Halifax Ref: {reference}"
        if ticker != asset_ticker:
            notes += f" | Halifax Ticker: {ticker}"

        # Build transaction row
        return [
            txn_date,                           # date
            None,                               # time
            self.account_name,                  # account
            self.account_type,                  # account_type
            transaction_type,                   # transaction_type
            category,                           # category
            amount,                             # amount
            'GBP',                              # currency
            asset_ticker,                       # asset_ticker
            units,                              # units
            price_per_unit,                     # price_per_unit
            notes,                              # notes
            'UK',                               # country
            None,                               # city
            False,                              # is_pension_contribution
            self.pdf_path.name,                 # data_source
            DataQuality.VERIFIED.value,         # data_quality (from official statement)
        ]

    def _parse_transaction_row(self, row: List) -> Optional[List]:
        """
        Parse a single transaction row from Halifax PDF.

        Expected columns:
        [0] Date - e.g., "28 Jan\n2026"
        [1] Type - "BUY" or "SELL"
        [2] Ticker - e.g., "VRXXC"
        [3] Listed On Market - e.g., "FUND"
        [4] Quantity - e.g., "36" or "117,692"
        [5] Executed Price (p) - e.g., "20,980.00" (in pence)
        [6] Net Consideration (£) - e.g., "7,633.79"
        [7] Reference - e.g., "21W2S38"

        Returns:
            Transaction row as list, or None if invalid
        """
        # Extract fields
        date_str = str(row[0]).strip() if row[0] else ""
        trade_type = str(row[1]).strip() if len(row) > 1 and row[1] else ""
        ticker = str(row[2]).strip() if len(row) > 2 and row[2] else ""
        market = str(row[3]).strip() if len(row) > 3 and row[3] else ""
        quantity_str = str(row[4]).strip() if len(row) > 4 and row[4] else ""
        price_pence_str = str(row[5]).strip() if len(row) > 5 and row[5] else ""
        net_consideration_str = str(row[6]).strip() if len(row) > 6 and row[6] else ""
        reference = str(row[7]).strip() if len(row) > 7 and row[7] else ""

        # Skip if no date or type
        if not date_str or not trade_type:
            return None

        # Parse date (Halifax format: "28 Jan\n2026" or "28 Jan 2026")
        try:
            # Remove newlines and parse
            date_clean = date_str.replace('\n', ' ')
            txn_date = pd.to_datetime(date_clean, format='%d %b %Y')
        except:
            print(f"  Warning: Could not parse date: {date_str}")
            return None

        # Parse transaction type
        if 'BUY' in trade_type.upper():
            transaction_type = TransactionType.BUY.value
            category = 'Fund Purchase' if 'FUND' in market else 'Stock Purchase'
        elif 'SELL' in trade_type.upper():
            transaction_type = TransactionType.SELL.value
            category = 'Fund Sale' if 'FUND' in market else 'Stock Sale'
        else:
            print(f"  Warning: Unknown trade type: {trade_type}")
            return None

        # Parse quantity
        try:
            units = float(quantity_str.replace(',', ''))
        except:
            units = None

        # Parse price (in pence, convert to pounds)
        try:
            price_pence = float(price_pence_str.replace(',', ''))
            price_per_unit = price_pence / 100  # Convert pence to pounds
        except:
            price_per_unit = None

        # Parse net consideration (already in pounds)
        try:
            amount = float(net_consideration_str.replace(',', ''))
            # Make buys negative (cash outflow), sells positive (cash inflow)
            if transaction_type == TransactionType.BUY.value:
                amount = -abs(amount)
            else:
                amount = abs(amount)
        except:
            amount = 0.0

        # Map Halifax ticker to standard ticker
        asset_ticker = halifax_ticker_to_standard(ticker)

        # Build notes
        notes = f"Halifax Ref: {reference}"
        if ticker != asset_ticker:
            notes += f" | Halifax Ticker: {ticker}"

        # Build transaction row
        return [
            txn_date,                           # date
            None,                               # time
            self.account_name,                  # account
            self.account_type,                  # account_type
            transaction_type,                   # transaction_type
            category,                           # category
            amount,                             # amount
            'GBP',                              # currency
            asset_ticker,                       # asset_ticker
            units,                              # units
            price_per_unit,                     # price_per_unit
            notes,                              # notes
            'UK',                               # country
            None,                               # city
            False,                              # is_pension_contribution
            self.pdf_path.name,                 # data_source
            DataQuality.VERIFIED.value,         # data_quality (from official statement)
        ]

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
            'transaction_types': self._transactions['transaction_type'].value_counts().to_dict(),
            'total_invested': self._transactions[self._transactions['transaction_type'] == TransactionType.BUY.value]['amount'].sum(),
            'total_proceeds': self._transactions[self._transactions['transaction_type'] == TransactionType.SELL.value]['amount'].sum(),
            'unique_assets': self._transactions['asset_ticker'].dropna().unique().tolist()
        }
