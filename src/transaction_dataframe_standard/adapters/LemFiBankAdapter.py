"""
LemFi Bank Adapter

Parses LemFi (RightCard Payment Services / ClearBank) savings-account statement
PDFs. LemFi issues a "Statement of Account" per account, all sharing the same
transaction table:

    DATE TYPE DESCRIPTION MONEY OUT MONEY IN BALANCE
    14 Jul, 2026 20:18 Credit Desc: Savings Funding £0.00 £100.00 £100
    17 Jul, 2026 06:43 Debit  Desc: Savings Withdrawal £10.00 £0.00 £90

There are two account types:
  - Instant Access Savings account (interest-bearing)
  - GBP (main) account — a pass-through wallet that receives money from John's
    external bank and forwards it to the savings account

The account a statement belongs to is auto-detected from its header, so all
LemFi PDFs can be passed to a single adapter and are kept as separate accounts.

Each row carries explicit Money Out / Money In / Balance columns, so direction
is unambiguous. Like the Monument savings accounts this holds John's own money:
funding/withdrawals are transfers between his accounts, and interest is the
only genuine new money.

Account Type: Savings
Data Quality: Verified (official bank PDF statements)
"""

import pandas as pd
import pdfplumber
import re
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    create_empty_standard_dataframe
)


class LemFiBankAdapter:
    """Adapter for LemFi savings-account statement PDFs."""

    # A transaction line: date, time, Credit/Debit, description, then the
    # Money Out, Money In and Balance columns (balance may omit decimals).
    _TXN_RE = re.compile(
        r'^(?P<date>\d{1,2}\s+\w{3},\s+\d{4})\s+'
        r'(?P<time>\d{1,2}:\d{2})\s+'
        r'(?P<type>Credit|Debit)\s+'
        r'(?P<desc>.+?)\s+'
        r'£(?P<out>[\d,]+\.\d{2})\s+'
        r'£(?P<in>[\d,]+\.\d{2})\s+'
        r'£(?P<bal>[\d,]+(?:\.\d{2})?)\s*$'
    )

    def __init__(self, pdf_paths, account_name: Optional[str] = None):
        """
        Args:
            pdf_paths: A single path or list of paths to LemFi statement PDFs.
            account_name: Force a single account name. If None (default), the
                account is auto-detected per statement (savings vs GBP main).
        """
        if isinstance(pdf_paths, (str, Path)):
            pdf_paths = [pdf_paths]
        self.pdf_paths = [Path(p) for p in pdf_paths]
        self.account_name = account_name
        self._transactions = self._parse_all_pdfs()

    def _detect_account_name(self, text: str) -> str:
        """Determine which LemFi account a statement belongs to from its header."""
        if self.account_name:
            return self.account_name
        if 'Savings Account' in text or 'Savings Account Overview' in text:
            return "LemFi Instant Access Savings"
        return "LemFi GBP Account"

    def _parse_all_pdfs(self) -> pd.DataFrame:
        all_transactions = []
        print(f"Parsing {len(self.pdf_paths)} LemFi statements...")

        for pdf_path in sorted(self.pdf_paths):
            try:
                txns = self._parse_single_pdf(pdf_path)
                all_transactions.extend(txns)
                print(f"  ✓ {pdf_path.name}: {len(txns)} transactions")
            except Exception as e:
                print(f"  ✗ Error parsing {pdf_path.name}: {e}")
                continue

        if not all_transactions:
            return create_empty_standard_dataframe()

        df = pd.DataFrame(all_transactions)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)
        print(f"  ✓ Total: {len(df)} LemFi transactions")
        return df

    def _parse_single_pdf(self, pdf_path: Path) -> List[dict]:
        with pdfplumber.open(pdf_path) as pdf:
            text = '\n'.join((p.extract_text() or '') for p in pdf.pages)

        account_name = self._detect_account_name(text)
        # The GBP wallet is a pass-through current account; the other is savings.
        account_type = (AccountType.SAVINGS.value if 'Savings' in account_name
                        else AccountType.CURRENT.value)

        transactions = []
        for line in text.split('\n'):
            m = self._TXN_RE.match(line.strip())
            if not m:
                continue

            money_out = self._to_float(m.group('out'))
            money_in = self._to_float(m.group('in'))
            amount = round(money_in - money_out, 2)
            if amount == 0:
                continue

            date = self._parse_date(m.group('date'))
            if date is None:
                continue

            desc = m.group('desc').strip()
            # Strip the "Desc:" label LemFi prefixes onto every description.
            desc = re.sub(r'^Desc:\s*', '', desc).strip()

            txn_type, category = self._classify(desc, amount)

            transactions.append({
                'date': date,
                'time': m.group('time'),
                'account': account_name,
                'account_type': account_type,
                'transaction_type': txn_type,
                'category': category,
                'amount': amount,
                'currency': 'GBP',
                'asset_ticker': None,
                'units': None,
                'price_per_unit': None,
                'notes': f"Payee: {account_name} | Description: {desc}",
                'country': 'UK',
                'city': None,
                'is_pension_contribution': False,
                'data_source': pdf_path.name,
                'data_quality': DataQuality.VERIFIED.value,
            })

        return transactions

    def _classify(self, desc: str, amount: float):
        """Return (transaction_type, category) for a line item."""
        d = desc.lower()
        if 'interest' in d:
            return (TransactionType.INTEREST.value, 'Interest Income')
        if amount > 0:
            return (TransactionType.TRANSFER.value, 'Transfer In')
        return (TransactionType.TRANSFER.value, 'Transfer Out')

    @staticmethod
    def _parse_date(raw: str):
        try:
            return datetime.strptime(raw.strip(), '%d %b, %Y').date()
        except ValueError:
            return None

    @staticmethod
    def _to_float(amount_str: str) -> float:
        return float(amount_str.replace('£', '').replace(',', '').strip())

    @property
    def transactions(self) -> pd.DataFrame:
        return self._transactions.copy()

    def get_summary(self) -> dict:
        df = self._transactions
        if df.empty:
            return {'total_transactions': 0}
        return {
            'total_transactions': len(df),
            'accounts': sorted(df['account'].unique().tolist()),
            'date_range': {'start': df['date'].min(), 'end': df['date'].max()},
            'total_interest': float(
                df[df['transaction_type'] == TransactionType.INTEREST.value]['amount'].sum()
            ),
        }
