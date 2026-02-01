"""
Hargreaves Lansdown Transactions Adapter

Parses HL quarterly investment report PDFs and extracts transaction data
into the comprehensive transaction standard format.

HL reports contain:
- Capital account transactions (buy/sell/transfers/interest)
- Income account transactions (dividends)
- Cash movements and balances
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
from ..sedol_ticker_map import sedol_to_ticker, is_sedol_mapped


class HLTransactionsAdapter:
    """
    Adapter for Hargreaves Lansdown quarterly PDF investment reports.

    Extracts transaction data from:
    - Capital Account Transactions (stocks, cash movements, interest)
    - Income Account Transactions (dividends)
    """

    def __init__(self, pdf_paths: List[str], account_name: str = "HL General", account_type: str = AccountType.GENERAL_INVESTMENT.value):
        """
        Initialize the HL adapter with one or more PDF report paths.

        Args:
            pdf_paths: List of paths to HL quarterly PDF reports
            account_name: Name to use for the HL account (e.g., "HL ISA", "HL SIPP")
            account_type: AccountType enum value
        """
        if isinstance(pdf_paths, str):
            pdf_paths = [pdf_paths]

        self.pdf_paths = [Path(p) for p in pdf_paths]
        self.account_name = account_name
        self.account_type = account_type

        # Parse all PDFs and combine transactions
        self._transactions = self._parse_all_pdfs()

    def _parse_all_pdfs(self) -> pd.DataFrame:
        """Parse all PDF files and combine into single dataframe."""
        all_transactions = []

        for pdf_path in self.pdf_paths:
            print(f"Parsing {pdf_path.name}...")
            transactions = self._parse_single_pdf(pdf_path)
            all_transactions.append(transactions)

        if all_transactions:
            combined = pd.concat(all_transactions, ignore_index=True)
            combined.sort_values('date', inplace=True)
            combined.reset_index(drop=True, inplace=True)
            return combined
        else:
            return create_empty_standard_dataframe()

    def _parse_single_pdf(self, pdf_path: Path) -> pd.DataFrame:
        """
        Parse a single HL quarterly PDF report.

        Args:
            pdf_path: Path to the PDF file

        Returns:
            DataFrame with transactions in comprehensive standard format
        """
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            # Extract transactions from each page
            for page_num, page in enumerate(pdf.pages):
                page_text = page.extract_text()

                # Find capital account transactions
                if "CAPITAL ACCOUNT TRANSACTIONS" in page_text:
                    capital_txns = self._extract_capital_transactions(page, pdf_path.name)
                    transactions.extend(capital_txns)

                # Find income account transactions (dividends)
                if "INCOME ACCOUNT TRANSACTIONS" in page_text:
                    income_txns = self._extract_income_transactions(page, pdf_path.name)
                    transactions.extend(income_txns)

        # Convert to DataFrame
        df = pd.DataFrame(transactions, columns=STANDARD_COLUMNS)

        # Set data types
        df['date'] = pd.to_datetime(df['date'], dayfirst=True, errors='coerce')
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['units'] = pd.to_numeric(df['units'], errors='coerce')
        df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
        df['is_pension_contribution'] = df['is_pension_contribution'].astype(bool)

        return df

    def _extract_capital_transactions(self, page, source_filename: str) -> List[List]:
        """
        Extract capital account transactions from a PDF page.

        These include:
        - Stock purchases/sales
        - Cash transfers
        - Interest payments
        - Fees
        """
        transactions = []

        # Extract table data
        tables = page.extract_tables()

        for table in tables:
            if not table:
                continue

            # Look for transaction table headers
            header_row = None
            for i, row in enumerate(table):
                if row and any("Transaction" in str(cell) for cell in row if cell):
                    header_row = i
                    break

            if header_row is None:
                continue

            # Process data rows
            for row in table[header_row + 1:]:
                if not row or len(row) < 3:
                    continue

                txn = self._parse_capital_transaction_row(row, source_filename)
                if txn:
                    transactions.append(txn)

        return transactions

    def _parse_capital_transaction_row(self, row: List, source_filename: str) -> Optional[List]:
        """
        Parse a single capital transaction row.

        Column structure varies by report date:
        - Newer reports (10 cols): [0]Date, [1]Trade Type, [2]Sedol, [3]Venue, [4]Details,
                                   [5]Units, [6]Empty, [7]Price Pence, [8]Value £, [9]Balance £
        - Older reports (12 cols): [0]Date, [1]Trade Type, [2]Sedol, [3]Venue, [4]Details,
                                   [5]Units, [6]Empty, [7]Price Pence, [8]Empty, [9]Value £, [10]Empty, [11]Balance £
        """
        # Skip empty or header rows
        if not row or not row[0]:
            return None

        # Try to parse date from first column
        date_str = str(row[0]).strip() if row[0] else ""
        if not date_str or "/" not in date_str:
            return None

        try:
            txn_date = pd.to_datetime(date_str, dayfirst=True)
        except:
            return None

        # Extract fields that are consistent across formats
        trade_type = str(row[1]).strip() if len(row) > 1 and row[1] else ""
        sedol = str(row[2]).strip() if len(row) > 2 and row[2] else ""
        venue = str(row[3]).strip() if len(row) > 3 and row[3] else ""
        details = str(row[4]).strip() if len(row) > 4 and row[4] else ""
        units_str = str(row[5]).strip() if len(row) > 5 and row[5] else ""
        unit_price_str = str(row[7]).strip() if len(row) > 7 and row[7] else ""

        # Value £ column position varies: index 8 for 10-column, index 9 for 12-column
        # Detect by checking if row has 10+ columns and which contains numeric value
        value_str = ""
        if len(row) >= 12:
            # Older format with 12 columns - Value at index 9
            value_str = str(row[9]).strip() if row[9] else ""
        elif len(row) >= 10:
            # Newer format with 10 columns - Value at index 8
            value_str = str(row[8]).strip() if row[8] else ""

        # Classify transaction type
        transaction_type, category = self._classify_transaction(trade_type, details)

        # Parse amounts
        try:
            units = float(units_str.replace(',', '')) if units_str else None
        except:
            units = None

        try:
            # Unit price in pence, convert to pounds
            if unit_price_str:
                price_pence = float(unit_price_str.replace(',', ''))
                price_per_unit = price_pence / 100 if price_pence > 0 else None
            else:
                price_per_unit = None
        except:
            price_per_unit = None

        try:
            # Value in pounds - handle negative amounts in parentheses
            if value_str:
                # Remove commas and handle (negative) format
                clean_value = value_str.replace(',', '').strip()
                if clean_value.startswith('(') and clean_value.endswith(')'):
                    # Negative amount
                    amount = -float(clean_value[1:-1])
                else:
                    amount = float(clean_value)
            else:
                amount = 0.0
        except:
            amount = 0.0

        # Get ticker from SEDOL
        asset_ticker = None
        if sedol and transaction_type in [TransactionType.BUY.value, TransactionType.SELL.value]:
            asset_ticker = sedol_to_ticker(sedol)

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
            details,                            # notes
            'UK',                               # country
            None,                               # city
            False,                              # is_pension_contribution (update if SIPP)
            source_filename,                    # data_source
            DataQuality.RECONSTRUCTED.value,    # data_quality (from PDF)
        ]

    def _extract_income_transactions(self, page, source_filename: str) -> List[List]:
        """
        Extract income account transactions (dividends) from a PDF page.
        """
        transactions = []

        tables = page.extract_tables()

        for table in tables:
            if not table:
                continue

            # Look for income table
            for i, row in enumerate(table):
                if not row:
                    continue

                # Check if this is an income transaction row
                if len(row) >= 3 and row[0] and "/" in str(row[0]):
                    txn = self._parse_income_transaction_row(row, source_filename)
                    if txn:
                        transactions.append(txn)

        return transactions

    def _parse_income_transaction_row(self, row: List, source_filename: str) -> Optional[List]:
        """
        Parse a single income transaction row.

        Expected columns: [0]Date, [1]Description, [2]Empty, [3]Payments £, [4]Receipts £, [5]Balance £
        """
        if not row or not row[0]:
            return None

        # Parse date
        date_str = str(row[0]).strip()
        try:
            txn_date = pd.to_datetime(date_str, dayfirst=True)
        except:
            return None

        description = str(row[1]).strip() if len(row) > 1 and row[1] else ""

        # Skip non-dividend transactions
        if "Dividend" not in description and "DIV" not in description.upper():
            return None

        # Parse amount (receipts column is at index 4)
        receipts_str = str(row[4]).strip() if len(row) > 4 and row[4] else ""
        try:
            amount = float(receipts_str.replace(',', '')) if receipts_str else 0.0
        except:
            amount = 0.0

        # Try to extract ticker from description
        asset_ticker = self._extract_ticker_from_description(description)

        return [
            txn_date,                           # date
            None,                               # time
            self.account_name,                  # account
            self.account_type,                  # account_type
            TransactionType.DIVIDEND.value,     # transaction_type
            'Investment Income',                # category
            amount,                             # amount (positive for dividends)
            'GBP',                              # currency
            asset_ticker,                       # asset_ticker
            None,                               # units (dividends don't change holdings)
            None,                               # price_per_unit
            description,                        # notes
            'UK',                               # country
            None,                               # city
            False,                              # is_pension_contribution
            source_filename,                    # data_source
            DataQuality.RECONSTRUCTED.value,    # data_quality
        ]

    def _classify_transaction(self, trade_type: str, details: str) -> tuple[str, str]:
        """
        Classify the transaction type and category based on trade type and details.

        Returns:
            (transaction_type, category)
        """
        trade_type_lower = trade_type.lower()
        details_lower = details.lower()

        if "sold" in trade_type_lower or "sell" in trade_type_lower:
            return (TransactionType.SELL.value, "Stock Sale")
        elif "bought" in trade_type_lower or "buy" in trade_type_lower:
            return (TransactionType.BUY.value, "Stock Purchase")
        elif "interest" in details_lower:
            return (TransactionType.INTEREST.value, "Interest Income")
        elif "dividend" in details_lower or "div:" in details_lower:
            return (TransactionType.DIVIDEND.value, "Investment Income")
        elif "payment to client" in details_lower or "withdrawal" in details_lower:
            return (TransactionType.WITHDRAWAL.value, "Cash Withdrawal")
        elif "receipt" in details_lower or "deposit" in details_lower:
            return (TransactionType.CONTRIBUTION.value, "Cash Deposit")
        elif "transfer" in details_lower:
            return (TransactionType.TRANSFER.value, "Transfer")
        elif "fee" in details_lower or "charge" in details_lower:
            return (TransactionType.FEE.value, "Platform Fee")
        else:
            return (TransactionType.TRANSFER.value, "Other")

    def _extract_ticker_from_description(self, description: str) -> Optional[str]:
        """
        Try to extract asset ticker from transaction description.

        This is a fallback for income transactions that may not have SEDOL codes.
        """
        # Common patterns in HL descriptions
        patterns = [
            r'\b([A-Z]{2,5})\b',  # 2-5 letter uppercase words (potential tickers)
        ]

        for pattern in patterns:
            match = re.search(pattern, description)
            if match:
                potential_ticker = match.group(1)
                # Filter out common non-ticker words
                if potential_ticker not in ['DIV', 'USD', 'GBP', 'INC', 'COM', 'CLASS']:
                    return potential_ticker

        return None

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
            'total_value': self._transactions['amount'].sum(),
            'unique_assets': self._transactions['asset_ticker'].dropna().unique().tolist()
        }
