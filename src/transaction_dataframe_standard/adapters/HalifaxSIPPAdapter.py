"""
Halifax SIPP Statements Adapter

Parses Halifax SIPP (Self-Invested Personal Pension) quarterly statements
and extracts transaction data into the comprehensive transaction standard format.

Halifax SIPP statements show:
- Cash movements (contributions, fees, purchases, sales)
- Holdings valuations

This adapter focuses on cash movements from the "CASH MOVEMENTS SCHEDULE" section.
"""

import pandas as pd
import pdfplumber
import re
from pathlib import Path
from typing import List
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    create_empty_standard_dataframe
)


class HalifaxSIPPAdapter:
    """
    Adapter for Halifax SIPP quarterly statement PDFs.

    Extracts transaction data from Cash Movements Schedule sections.
    """

    def __init__(self, pdf_paths: List[str], account_name: str = "Halifax SIPP"):
        """
        Initialize the Halifax SIPP adapter with PDF statement paths.

        Args:
            pdf_paths: List of paths to Halifax SIPP statement PDFs
            account_name: Name to use for the account
        """
        self.pdf_paths = [Path(p) for p in pdf_paths]
        self.account_name = account_name

        # Parse all PDFs
        self._transactions = self._parse_pdfs()

    def _parse_pdfs(self) -> pd.DataFrame:
        """Parse all SIPP PDFs and combine transactions."""
        all_transactions = []

        for pdf_path in sorted(self.pdf_paths):
            transactions = self._parse_single_pdf(pdf_path)
            all_transactions.extend(transactions)

        if not all_transactions:
            return create_empty_standard_dataframe()

        df = pd.DataFrame(all_transactions)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        return df

    def _parse_single_pdf(self, pdf_path: Path) -> List[dict]:
        """Parse a single SIPP statement PDF."""
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()

                if 'CASH MOVEMENTS SCHEDULE' not in text:
                    continue

                lines = text.split('\n')
                table_started = False

                for line in lines:
                    # Find table start
                    if 'Date Description Debits' in line or 'Date Description Credits' in line:
                        table_started = True
                        continue

                    if not table_started:
                        continue

                    # Stop at end markers
                    if any(marker in line for marker in ['BALANCE CARRIED FORWARD', 'TOTAL PAID OUT', 'Please note -']):
                        break

                    # Skip headers
                    if 'BALANCE BROUGHT FORWARD' in line:
                        continue

                    # Parse transaction line
                    txn = self._parse_transaction_line(line, pdf_path.name)
                    if txn:
                        transactions.append(txn)

        return transactions

    def _parse_transaction_line(self, line: str, source_file: str) -> dict:
        """Parse a single transaction line from Cash Movements Schedule."""
        # Match date at start: DD/MM/YYYY
        date_match = re.match(r'(\d{2}/\d{2}/\d{4})\s+(.*)', line)
        if not date_match:
            return None

        date_str = date_match.group(1)
        rest = date_match.group(2).strip()

        try:
            date = datetime.strptime(date_str, '%d/%m/%Y').date()
        except:
            return None

        # Determine transaction type and extract details
        txn_type, category, amount, asset_ticker, units, notes = self._classify_transaction(rest)

        if txn_type is None:
            return None

        return {
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': AccountType.SIPP.value,
            'transaction_type': txn_type,
            'category': category,
            'amount': amount,
            'currency': 'GBP',
            'asset_ticker': asset_ticker,
            'units': units,
            'price_per_unit': abs(amount / units) if units and units != 0 else None,
            'notes': notes,
            'country': 'UK',
            'city': None,
            'is_pension_contribution': txn_type == TransactionType.INCOME.value and 'contribution' in rest.lower(),
            'data_source': source_file,
            'data_quality': DataQuality.VERIFIED.value
        }

    def _classify_transaction(self, description: str) -> tuple:
        """
        Classify SIPP transaction and extract details.

        Returns: (transaction_type, category, amount, asset_ticker, units, notes)
        """
        desc_lower = description.lower()

        # Extract amounts: look for transaction amount (before balance)
        # Pattern: "Description AMOUNT BALANCE(cr|dr)"
        # We want the AMOUNT, and determine sign from context (Buy/Fee = negative, Sale/Income = positive)
        amounts = re.findall(r'(\d+(?:\.\d{2})?)', description)

        if len(amounts) < 2:
            return (None, None, None, None, None, None)

        # The transaction amount is typically the second-to-last number
        amount = float(amounts[-2]) if len(amounts) >= 2 else float(amounts[0])

        # Determine sign from transaction type (will adjust after classification)
        is_debit = None  # Will be determined by transaction type

        # Purchase: "Purchase of 69808.51 HSBC INDEX TRACKER INVESTMENT 177323.12 0.01cr"
        if 'purchase of' in desc_lower:
            units_match = re.search(r'purchase of\s+([\d.]+)\s+(.+?)\s+\d', desc_lower)
            if units_match:
                units = float(units_match.group(1))
                fund_name = units_match.group(2).strip()
                asset_ticker = self._map_fund_to_ticker(fund_name)

                return (
                    TransactionType.BUY.value,
                    'Fund Purchase',
                    -abs(amount),  # Purchases are negative (cash out)
                    asset_ticker,
                    units,
                    description
                )

        # Sale: "Sale of 43.37 VANGUARD INVESTMENT SERIES PLC 15350.14 15318.63cr"
        if 'sale of' in desc_lower:
            units_match = re.search(r'sale of\s+([\d.]+)\s+(.+?)\s+\d', desc_lower)
            if units_match:
                units = float(units_match.group(1))
                fund_name = units_match.group(2).strip()
                asset_ticker = self._map_fund_to_ticker(fund_name)

                return (
                    TransactionType.SELL.value,
                    'Fund Sale',
                    abs(amount),  # Sales are positive (cash in)
                    asset_ticker,
                    units,
                    description
                )

        # Fee: "FEE cash type: SIPP Fee Q3-2023 45.00 44.99dr"
        if 'fee' in desc_lower and 'sipp fee' in desc_lower:
            return (
                TransactionType.EXPENSE.value,
                'SIPP Platform Fee',
                -abs(amount),  # Fees are negative (cash out)
                None,
                None,
                description
            )

        # Transfer IN / Contribution: "AJ Bell Request ... Transfer Valu 177323.13 177323.13cr"
        if 'aj bell request' in desc_lower:
            if 'transfer' in desc_lower or amount > 1000:  # Large amount = likely initial transfer
                return (
                    TransactionType.INCOME.value,
                    'Pension Transfer',
                    abs(amount),  # Transfers in are positive (cash in)
                    None,
                    None,
                    description
                )
            elif 'contribution' in desc_lower:
                return (
                    TransactionType.INCOME.value,
                    'Pension Contribution',
                    abs(amount),  # Contributions are positive (cash in)
                    None,
                    None,
                    description
                )
            elif 'tax relief' in desc_lower:
                return (
                    TransactionType.INCOME.value,
                    'Tax Relief',
                    abs(amount),  # Tax relief is positive (cash in)
                    None,
                    None,
                    description
                )
            else:
                # Small AJ Bell requests are likely fees
                return (
                    TransactionType.EXPENSE.value,
                    'Platform Fee',
                    -abs(amount),  # Fees are negative (cash out)
                    None,
                    None,
                    description
                )

        # Interest: "Credit Interest 103.48 13.49cr"
        if 'interest' in desc_lower:
            # Interest can be positive or negative (rare)
            # Use abs and check if it's a debit interest charge
            interest_amount = abs(amount)
            if 'debit interest' in desc_lower or ('dr' in description and 'interest' in desc_lower):
                interest_amount = -interest_amount

            return (
                TransactionType.INTEREST.value,
                'Interest Income',
                interest_amount,
                None,
                None,
                description
            )

        # Unclassified
        return (None, None, None, None, None, None)

    def _map_fund_to_ticker(self, fund_name: str) -> str:
        """Map SIPP fund names to tickers."""
        fund_lower = fund_name.lower()

        # HSBC INDEX TRACKER INVESTMENT FUNDS - This is the same as Halifax's MDAABG
        # (HSBC FTSE All World Index Fund - verified by matching purchase prices)
        if 'hsbc' in fund_lower and 'index tracker' in fund_lower:
            return 'MDAABG'

        # VANGUARD INVESTMENT SERIES PLC
        if 'vanguard investment series' in fund_lower:
            # This is VFEG-OEIC based on the sale price (£354/unit matches VFEG-OEIC, not VUSA-OEIC £955/unit)
            return 'VFEG-OEIC'

        # Return unknown ticker with prefix
        return f'SIPP:{fund_name[:20].upper().replace(" ", "-")}'

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the parsed transactions DataFrame."""
        return self._transactions
