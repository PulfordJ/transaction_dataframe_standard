"""
Halifax Cash History Adapter

Parses Halifax Cash History PDFs (cash_and_transactions.pdf) which include:
- All cash movements (Funds Transfer In, Additional Subscription, Credit Payment, etc.)
- Purchases with units and costs
- Sales with units and proceeds
- Dividends (Internal movement from income statement)
- Fees (Customer Administration Fee, SIPP Quarter Fee, etc.)
- Running balance

This is the comprehensive transaction data source for Halifax accounts.
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


class HalifaxCashHistoryAdapter:
    """
    Adapter for Halifax Cash History PDFs.

    Extracts comprehensive transaction data including cash movements,
    purchases, sales, dividends, and fees.
    """

    def __init__(self, pdf_path: str, account_name: str, account_type: str):
        """
        Initialize the Halifax Cash History adapter.

        Args:
            pdf_path: Path to Halifax cash_and_transactions.pdf
            account_name: Name to use for the account (e.g., "Halifax ISA")
            account_type: Type of account (e.g., "ISA", "SIPP", "General Investment")
        """
        self.pdf_path = Path(pdf_path)
        self.account_name = account_name
        self.account_type = account_type

        # Parse PDF
        self._transactions = self._parse_pdf()

    def _parse_pdf(self) -> pd.DataFrame:
        """Parse cash history PDF and extract transactions."""
        transactions = []

        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()

                if not text:
                    continue

                lines = text.split('\n')

                for line in lines:
                    # Skip headers and non-data lines
                    if any(x in line for x in ['Date Description', 'Balance Brought Forward',
                                                'Back Print', 'CASH HISTORY', 'Search Period',
                                                'Account code', 'Personal reference', 'Account status']):
                        continue

                    # Parse transaction line
                    txn = self._parse_transaction_line(line)
                    if txn:
                        transactions.append(txn)

        if not transactions:
            return create_empty_standard_dataframe()

        df = pd.DataFrame(transactions)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date')

        return df

    def _parse_transaction_line(self, line: str) -> dict:
        """Parse a single transaction line from Cash History."""
        # Match date at start: DD MMM YYYY (e.g., "08 Aug 2023")
        date_match = re.match(r'(\d{2}\s+\w{3}\s+\d{4})\s+(.*)', line)
        if not date_match:
            return None

        date_str = date_match.group(1)
        rest = date_match.group(2).strip()

        try:
            date = datetime.strptime(date_str, '%d %b %Y').date()
        except:
            return None

        # Classify transaction and extract details
        txn_type, category, amount, asset_ticker, units, notes = self._classify_transaction(rest)

        if txn_type is None:
            return None

        return {
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self._map_account_type(),
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
            'is_pension_contribution': self.account_type == 'SIPP' and txn_type == TransactionType.INCOME.value,
            'data_source': self.pdf_path.name,
            'data_quality': DataQuality.VERIFIED.value
        }

    def _classify_transaction(self, description: str) -> tuple:
        """
        Classify transaction and extract details.

        Returns: (transaction_type, category, amount, asset_ticker, units, notes)
        """
        desc_lower = description.lower()

        # Extract amounts from end of line: debit, credit, balance
        # Pattern: "Description £amount - £balance" or "Description - £amount £balance"
        # Updated to handle negative balances like "£-60.00" (minus after pound sign)
        amounts = re.findall(r'£-?([\d,]+\.\d{2})', description)

        if len(amounts) < 2:
            return (None, None, None, None, None, None)

        # Cash Transfer In: "Funds Transfer In - £9,999.00 £9,999.00"
        if 'funds transfer in' in desc_lower or 'credit payment' in desc_lower or 'additional subscription' in desc_lower:
            amount_str = amounts[-2]  # Second to last is credit amount
            amount = float(amount_str.replace(',', ''))

            category = 'Investment Account Transfer'
            if 'additional subscription' in desc_lower:
                category = 'ISA Contribution'
            elif 'credit payment' in desc_lower:
                category = 'Investment Deposit'

            return (
                TransactionType.INCOME.value,
                category,
                amount,
                None,
                None,
                description
            )

        # Funds Withdrawal: "Funds Withdrawal £10.00 - £0.00"
        if 'funds withdrawal' in desc_lower:
            amount_str = amounts[-2]  # Second to last is debit amount
            amount = -float(amount_str.replace(',', ''))

            return (
                TransactionType.EXPENSE.value,
                'Investment Withdrawal',
                amount,
                None,
                None,
                description
            )

        # Purchase: "Purchase 61492.16 HSBC IDX TKR INV FTSE ALL £156,998.99 - £0.01"
        if 'purchase' in desc_lower or 'buy' in desc_lower:
            # Extract units (first number after "Purchase")
            units_match = re.search(r'purchase\s+([\d,]+\.?\d*)\s+', desc_lower)
            if units_match:
                units = float(units_match.group(1).replace(',', ''))

                # Extract fund name (between units and amount)
                fund_match = re.search(r'purchase\s+[\d,]+\.?\d*\s+(.+?)\s+£', description, re.IGNORECASE)
                fund_name = fund_match.group(1).strip() if fund_match else ''

                asset_ticker = self._map_fund_to_ticker(fund_name)

                # Amount is negative (cash out)
                amount_str = amounts[-2]
                amount = -float(amount_str.replace(',', ''))

                return (
                    TransactionType.BUY.value,
                    'Fund Purchase',
                    amount,
                    asset_ticker,
                    units,
                    description
                )

        # Sale: "Sale 928 VANGUARD FUNDS PLC FTSE DEVLPD - £18,808.16 £45,690.95"
        if 'sale' in desc_lower or 'sell' in desc_lower:
            # Extract units
            units_match = re.search(r'sale\s+([\d,]+\.?\d*)\s+', desc_lower)
            if units_match:
                units = float(units_match.group(1).replace(',', ''))

                # Extract fund name
                fund_match = re.search(r'sale\s+[\d,]+\.?\d*\s+(.+?)\s+-\s+£', description, re.IGNORECASE)
                fund_name = fund_match.group(1).strip() if fund_match else ''

                asset_ticker = self._map_fund_to_ticker(fund_name)

                # Amount is positive (cash in)
                amount_str = amounts[-2]
                amount = float(amount_str.replace(',', ''))

                return (
                    TransactionType.SELL.value,
                    'Fund Sale',
                    amount,
                    asset_ticker,
                    units,
                    description
                )

        # Dividend: "Internal movement from income statement - Income - £133.52 £1,334.56"
        if 'internal movement from income statement' in desc_lower or 'income sweep' in desc_lower:
            amount_str = amounts[-2]
            amount = float(amount_str.replace(',', ''))

            return (
                TransactionType.DIVIDEND.value,
                'Dividend Income',
                amount,
                None,
                None,
                description
            )

        # Interest: "Credit Interest - Gross - £103.48 £13.49"
        if 'credit interest' in desc_lower:
            amount_str = amounts[-2]
            amount = float(amount_str.replace(',', ''))

            return (
                TransactionType.INTEREST.value,
                'Interest Income',
                amount,
                None,
                None,
                description
            )

        # Fees: "Customer Administration Fee £36.00 - £0.02" or "SIPP Quarter Fee Debit £45.00 - £-44.99"
        # Also matches fees without "Debit" keyword: "Apr 2024-Mar 2025 Customer Administration Fee £35.98 - £0.00"
        if 'fee' in desc_lower and ('debit' in desc_lower or 'administration' in desc_lower or 'sipp' in desc_lower):
            amount_str = amounts[-2]
            amount = -float(amount_str.replace(',', ''))

            category = 'Platform Fee'
            if 'sipp' in desc_lower:
                category = 'SIPP Platform Fee'
            elif 'administration' in desc_lower:
                category = 'Administration Fee'

            return (
                TransactionType.EXPENSE.value,
                category,
                amount,
                None,
                None,
                description
            )

        # Credit/Debit Adjustment
        if 'credit adjustment' in desc_lower:
            amount_str = amounts[-2]
            amount = float(amount_str.replace(',', ''))

            return (
                TransactionType.INCOME.value,
                'Account Adjustment',
                amount,
                None,
                None,
                description
            )

        if 'debit adjustment' in desc_lower:
            amount_str = amounts[-2]
            amount = -float(amount_str.replace(',', ''))

            return (
                TransactionType.EXPENSE.value,
                'Account Adjustment',
                amount,
                None,
                None,
                description
            )

        # Unclassified
        return (None, None, None, None, None, None)

    def _map_fund_to_ticker(self, fund_name: str) -> str:
        """Map Halifax fund names to tickers."""
        fund_lower = fund_name.lower()

        # HSBC FTSE All World Index Fund
        if 'hsbc' in fund_lower and ('all world' in fund_lower or 'ftse all' in fund_lower or 'idx tkr' in fund_lower):
            return 'MDAABG'

        # Vanguard FTSE Developed Asia Pacific ex Japan
        if 'vanguard' in fund_lower and ('asia pac' in fund_lower or 'pacific ex' in fund_lower or 'devlpd asia' in fund_lower):
            return 'VAPX'

        # Vanguard FTSE 100 Index
        if 'vanguard' in fund_lower and ('ftse 100' in fund_lower or 'uk lt vanguard' in fund_lower or 'vanguard inv uk lt' in fund_lower):
            return 'VUKE-OEIC'

        # Vanguard US Equity Index
        if 'vanguard' in fund_lower and ('us eq' in fund_lower or 'us equity' in fund_lower or 'vanguard inv fds van' in fund_lower):
            return 'VUSA-OEIC'

        # Vanguard LifeStrategy funds (for SIPP)
        if 'vanguard invs srs' in fund_lower or 'vanguard pac exjpn' in fund_lower:
            return 'VFEG-OEIC'

        # Vanguard FTSE Developed Asia Pacific ex Japan (alternative names)
        if 'vanguard funds plc' in fund_lower and ('ftse' in fund_lower or 'devlpd' in fund_lower):
            return 'VAPX'

        # Return unknown ticker
        return f'HALIFAX:{fund_name[:20].upper().replace(" ", "-")}'

    def _map_account_type(self) -> str:
        """Map account type string to AccountType enum."""
        if self.account_type == 'ISA':
            return AccountType.ISA.value
        elif self.account_type == 'SIPP':
            return AccountType.SIPP.value
        else:
            return AccountType.GENERAL_INVESTMENT.value

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the parsed transactions DataFrame."""
        return self._transactions
