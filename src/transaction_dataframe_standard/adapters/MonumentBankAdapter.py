"""
Monument Bank Adapter

Parses Monument Bank savings-account statement PDFs. Monument issues a monthly
statement per account (Easy Access Savings, 60 Day Notice, etc.) plus a one-off
closing statement for fixed-term deposits. Every line item is one of:

- Deposit from <sort code> | <account>   -> money in  (Transfer)
- Withdrawal to <sort code> | <account>  -> money out (Transfer)
- Transfer to/from <sort code> | <acct>  -> internal / linked-account move
- Interest applied / Interest paid        -> money in  (Interest)
- Interest rate changed to X%             -> informational only (skipped)

These are savings accounts holding John's own money, so every cash movement is a
transfer between his accounts (typically to/from his linked Monzo account, sort
code 04-00-04) rather than spending. Interest is the only genuine new money.

Three statement layouts are handled transparently:
  A. Fixed-term closing statement  -> dates as DD/MM/YYYY
  B. Monthly "Your Account Statement" ("Transactions in the period:")
  C. Monthly "Account Statement" overview (newest template)
Layouts B and C use "DD Month YYYY" dates.

Annual Interest Statements have a different (summary) layout and carry no
per-transaction cash movements, so they are ignored.

Account Type: Savings
Data Quality: Verified (official bank PDF statements)
"""

import pandas as pd
import pdfplumber
import re
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    create_empty_standard_dataframe
)


class MonumentBankAdapter:
    """Adapter for Monument Bank savings-account statement PDFs."""

    # Known Monument accounts -> friendly account names used across the app.
    ACCOUNT_NAMES = {
        '00330435': 'Monument Easy Access Savings',
        '00324731': 'Monument 60 Day Notice',
        '00085401': 'Monument 6 Month Fixed Term',
    }

    # Sort code of John's linked Monzo current account.
    _MONZO_SORT_CODE = '04-00-04'
    # Monument's own sort code (internal account-to-account movements).
    _MONUMENT_SORT_CODE = '04-13-67'

    # A transaction line: a date, a description, then one or two £ amounts.
    # Money-movement rows carry two amounts (the In/Out value + running balance);
    # informational rows (e.g. "Interest rate changed") carry only the balance.
    _TXN_RE = re.compile(
        r'^(?P<date>\d{1,2}/\d{2}/\d{4}|\d{1,2}\s+[A-Za-z]+\s+\d{4})\s+'
        r'(?P<desc>.+?)\s+'
        r'(?P<amounts>£[\d,]+\.\d{2}(?:\s+£[\d,]+\.\d{2})?)\s*$'
    )
    _ACCOUNT_RE = re.compile(r'Account number:?\s*(\d{8})')
    _OPENING_RE = re.compile(r'Opening [Bb]alance:?\s*£([\d,]+\.\d{2})')
    _COUNTERPARTY_RE = re.compile(r'(\d{2}-\d{2}-\d{2})\s*\|')

    def __init__(self, pdf_paths, account_name_prefix: str = "Monument"):
        """
        Args:
            pdf_paths: A single path or list of paths to Monument statement PDFs.
            account_name_prefix: Fallback prefix for accounts not in ACCOUNT_NAMES.
        """
        if isinstance(pdf_paths, (str, Path)):
            pdf_paths = [pdf_paths]
        self.pdf_paths = [Path(p) for p in pdf_paths]
        self.account_name_prefix = account_name_prefix
        self._transactions = self._parse_all_pdfs()

    def _parse_all_pdfs(self) -> pd.DataFrame:
        all_transactions = []
        print(f"Parsing {len(self.pdf_paths)} Monument Bank statements...")

        for pdf_path in sorted(self.pdf_paths):
            # Annual interest statements have no per-transaction cash movements.
            if 'Annual Interest' in pdf_path.name:
                continue
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
        print(f"  ✓ Total: {len(df)} Monument transactions")
        return df

    def _parse_single_pdf(self, pdf_path: Path) -> List[dict]:
        with pdfplumber.open(pdf_path) as pdf:
            text = '\n'.join((p.extract_text() or '') for p in pdf.pages)

        if 'Annual Statement of Interest' in text:
            return []

        # Identify the account this statement belongs to.
        acct_match = self._ACCOUNT_RE.search(text)
        account_number = acct_match.group(1) if acct_match else None
        account_name = self._resolve_account_name(account_number, text)

        opening_match = self._OPENING_RE.search(text)
        prev_balance = self._to_float(opening_match.group(1)) if opening_match else None

        transactions = []
        for line in text.split('\n'):
            m = self._TXN_RE.match(line.strip())
            if not m:
                continue

            amounts = re.findall(r'£([\d,]+\.\d{2})', m.group('amounts'))
            # Only balance shown (no In/Out value) -> informational row, skip.
            if len(amounts) < 2:
                continue

            desc = m.group('desc').strip()
            if 'interest rate changed' in desc.lower():
                continue

            txn_amount = self._to_float(amounts[0])
            balance = self._to_float(amounts[-1])

            date = self._parse_date(m.group('date'))
            if date is None:
                continue

            txn_type, category, direction = self._classify(desc)

            # Prefer the running-balance delta to determine sign; it is
            # self-validating and independent of description wording.
            if prev_balance is not None:
                delta = round(balance - prev_balance, 2)
                if delta != 0:
                    direction = 1 if delta > 0 else -1
                prev_balance = balance

            amount = txn_amount * direction

            transactions.append({
                'date': date,
                'time': None,
                'account': account_name,
                'account_type': AccountType.SAVINGS.value,
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

    def _resolve_account_name(self, account_number: Optional[str], text: str) -> str:
        if account_number and account_number in self.ACCOUNT_NAMES:
            return self.ACCOUNT_NAMES[account_number]
        # Fall back to the product/account name printed on the statement.
        m = re.search(r'Product name:\s*(.+)', text)
        if not m:
            m = re.search(r'Account name\s+(.+)', text)
        if m:
            return f"{self.account_name_prefix} {m.group(1).strip()}"
        if account_number:
            return f"{self.account_name_prefix} {account_number}"
        return self.account_name_prefix

    def _classify(self, desc: str) -> Tuple[str, str, int]:
        """Return (transaction_type, category, direction) for a line item.

        direction is +1 for money in and -1 for money out; it is only a
        keyword-based fallback and may be overridden by the balance delta.
        """
        d = desc.lower()

        if 'interest applied' in d or 'interest paid' in d:
            return (TransactionType.INTEREST.value, 'Interest Income', 1)

        counterparty = self._COUNTERPARTY_RE.search(desc)
        sort_code = counterparty.group(1) if counterparty else None

        # Money in: deposits / transfers into the account.
        if any(k in d for k in ('deposit from', 'transfer from', 'credit')):
            if sort_code == self._MONUMENT_SORT_CODE:
                return (TransactionType.TRANSFER.value, 'Internal Transfer', 1)
            if sort_code == self._MONZO_SORT_CODE:
                return (TransactionType.TRANSFER.value, 'Transfer from Monzo', 1)
            return (TransactionType.TRANSFER.value, 'Transfer In', 1)

        # Money out: withdrawals / transfers out of the account.
        if any(k in d for k in ('withdrawal to', 'transfer to', 'debit')):
            if sort_code == self._MONUMENT_SORT_CODE:
                return (TransactionType.TRANSFER.value, 'Internal Transfer', -1)
            if sort_code == self._MONZO_SORT_CODE:
                return (TransactionType.TRANSFER.value, 'Transfer to Monzo', -1)
            return (TransactionType.TRANSFER.value, 'Transfer Out', -1)

        # Unknown wording: default to a transfer; the balance delta fixes sign.
        return (TransactionType.TRANSFER.value, 'Account Transfer', 1)

    @staticmethod
    def _parse_date(raw: str):
        raw = raw.strip()
        for fmt in ('%d/%m/%Y', '%d %B %Y'):
            try:
                return datetime.strptime(raw, fmt).date()
            except ValueError:
                continue
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
