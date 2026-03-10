"""
M&S Credit Card Adapter

Parses M&S Bank credit card statement PDFs which include:
- Purchases (negative amounts): Store purchases, online transactions
- Payment receipts (positive amounts with CR): Direct Debit, manual payments
- Fees: Non-sterling transaction fees, late payment fees, interest charges
- Foreign currency conversions
- Running balance

Account: M&S Credit Card (5299 3010 9152 2459)
Account Type: Credit Card
"""

import pandas as pd
import pdfplumber
import re
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime

from ..standard import (
    TransactionType, AccountType, DataQuality,
    create_empty_standard_dataframe
)


class MSCreditCardAdapter:
    """
    Adapter for M&S Credit Card statement PDFs.

    Extracts transaction data from monthly PDF statements including
    purchases, payments, fees, and foreign currency transactions.
    """

    def __init__(self, pdf_paths, account_name: str = "M&S Credit Card"):
        """
        Initialize M&S Credit Card adapter.

        Args:
            pdf_paths: List of paths to M&S statement PDFs (or single path)
            account_name: Name for the account (default: "M&S Credit Card")
        """
        # Handle single path or list of paths
        if isinstance(pdf_paths, str):
            pdf_paths = [pdf_paths]

        self.pdf_paths = [Path(p) for p in pdf_paths]
        self.account_name = account_name
        self.account_number = "5299 3010 9152 2459"

        # Parse all PDFs
        self._transactions = self._parse_all_pdfs()

    def _parse_all_pdfs(self) -> pd.DataFrame:
        """Parse all PDF statements and combine into single DataFrame."""
        all_transactions = []

        print(f"Parsing {len(self.pdf_paths)} M&S Credit Card statements...")

        for pdf_path in sorted(self.pdf_paths):
            try:
                transactions = self._parse_single_pdf(pdf_path)
                all_transactions.extend(transactions)
                print(f"  ✓ {pdf_path.name}: {len(transactions)} transactions")
            except Exception as e:
                print(f"  ✗ Error parsing {pdf_path.name}: {e}")
                continue

        if not all_transactions:
            return create_empty_standard_dataframe()

        df = pd.DataFrame(all_transactions)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        print(f"  ✓ Total: {len(df)} M&S transactions")
        return df

    def _parse_single_pdf(self, pdf_path: Path) -> List[dict]:
        """Parse a single M&S PDF statement."""
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                page_transactions = self._parse_page_text(text, pdf_path)
                transactions.extend(page_transactions)

        return transactions

    def _parse_page_text(self, text: str, source_file: Path) -> List[dict]:
        """Parse transaction lines from page text."""
        transactions = []
        lines = text.split('\n')

        # Find transaction section
        in_transaction_section = False
        prev_transaction = None

        for line in lines:
            # Start of transaction section
            if 'Date Date of' in line or 'Applied Transaction Description Amount' in line:
                in_transaction_section = True
                continue

            # End of transaction section
            if in_transaction_section and any(x in line for x in [
                'Present balance', 'Available to spend', 'Principal Balance',
                'Minimum payment', 'Payment due date', 'RATES OF INTEREST',
                'Balance from previous statement', 'Thank you for paying'
            ]):
                in_transaction_section = False
                continue

            if not in_transaction_section:
                continue

            # Skip non-transaction lines
            if any(x in line for x in [
                'M&S points total',
                'see overleaf',
                'Transactions shown on this statement'
            ]):
                continue

            # Try to parse as transaction
            txn = self._parse_transaction_line(line, source_file)
            if txn:
                transactions.append(txn)
                prev_transaction = txn
            elif prev_transaction:
                # Check if this is a continuation line (FX conversion)
                if '@' in line and 'Exchange Rate' not in line:
                    # Foreign currency detail: "16.00 USD@1.2559"
                    prev_transaction['notes'] += f" | FX: {line.strip()}"
                elif 'Exchange Rate' in line:
                    # Exchange rate note
                    prev_transaction['notes'] += f" | {line.strip()}"
                elif 'Non-Sterling Transaction Fee' in line:
                    # Fee continuation - parse as separate transaction
                    fee_txn = self._parse_fee_line(line, prev_transaction, source_file)
                    if fee_txn:
                        transactions.append(fee_txn)

        return transactions

    def _parse_transaction_line(self, line: str, source_file: Path) -> Optional[dict]:
        """
        Parse a single transaction line.

        Format: "DD MMM YY DD MMM YY Description £amount [CR]"
        Example: "26 Nov 24 26 Nov 24 Direct Debit - Thank You £9.90 CR"
        """
        # Regex pattern for M&S transaction line
        # Group 1: Date Applied (DD MMM YY)
        # Group 2: Date of Transaction (DD MMM YY)
        # Group 3: Description
        # Group 4: Amount (£X.XX or £X.XX CR)
        pattern = r'^(\d{2}\s+\w{3}\s+\d{2})\s+(\d{2}\s+\w{3}\s+\d{2})\s+(.+?)\s+(£[\d,]+\.\d{2}(?:\s*CR)?)\s*$'

        match = re.match(pattern, line)
        if not match:
            return None

        date_applied_str = match.group(1)
        date_transaction_str = match.group(2)
        description = match.group(3).strip()
        amount_str = match.group(4)

        # Parse date (use Date of Transaction, not Date Applied)
        try:
            date = datetime.strptime(date_transaction_str, '%d %b %y').date()
        except:
            return None

        # Parse amount
        amount, is_credit = self._parse_amount(amount_str)

        # Classify transaction
        txn_type, category = self._classify_transaction(description, is_credit, amount)

        return {
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': AccountType.CREDIT_CARD.value,
            'transaction_type': txn_type,
            'category': category,
            'amount': amount,
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"{description} | Date Applied: {date_applied_str}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': source_file.name,
            'data_quality': DataQuality.VERIFIED.value
        }

    def _parse_amount(self, amount_str: str) -> Tuple[float, bool]:
        """Parse amount and determine if it's a credit (payment receipt)."""
        is_credit = 'CR' in amount_str.upper()
        clean = amount_str.replace('£', '').replace('CR', '').replace('cr', '').replace(',', '').strip()
        amount = float(clean)

        # Apply correct sign:
        # - Credits (payments IN to M&S) are POSITIVE (reduce debt)
        # - Debits (purchases) are NEGATIVE (increase debt)
        return (amount if is_credit else -amount, is_credit)

    def _classify_transaction(self, description: str, is_credit: bool, amount: float) -> Tuple[str, str]:
        """
        Classify M&S transaction type and category.

        Rules:
        - Payment receipts (CR): Always Expense (will be reclassified to Transfer if matched)
        - Purchases (DR): Always Expense
        - Fees: Expense
        - Interest: Interest (expense)
        """
        desc_lower = description.lower()

        if is_credit:
            # Payment receipts (positive amounts with CR)
            # Initially classify as Expense - will be reclassified to Transfer if matched with Monzo
            if 'direct debit' in desc_lower:
                return (TransactionType.EXPENSE.value, 'Credit Card Payment - Direct Debit')
            elif 'payment' in desc_lower or 'thank you' in desc_lower:
                return (TransactionType.EXPENSE.value, 'Credit Card Payment - Manual')
            else:
                return (TransactionType.EXPENSE.value, 'Credit Card Payment')
        else:
            # Purchases and fees (negative amounts)
            if 'fee' in desc_lower:
                if 'non-sterling' in desc_lower or 'foreign' in desc_lower:
                    return (TransactionType.EXPENSE.value, 'Foreign Transaction Fee')
                elif 'late' in desc_lower:
                    return (TransactionType.EXPENSE.value, 'Late Payment Fee')
                else:
                    return (TransactionType.EXPENSE.value, 'Credit Card Fee')
            elif 'interest' in desc_lower:
                return (TransactionType.INTEREST.value, 'Credit Card Interest')
            else:
                # Regular purchase
                return (TransactionType.EXPENSE.value, 'Credit Card Purchase')

    def _parse_fee_line(self, line: str, related_txn: dict, source_file: Path) -> Optional[dict]:
        """Parse fee continuation line (e.g., Non-Sterling Transaction Fee)."""
        # Extract fee amount
        amount_match = re.search(r'£([\d,]+\.\d{2})', line)
        if not amount_match:
            return None

        amount = -float(amount_match.group(1).replace(',', ''))

        return {
            'date': related_txn['date'],
            'time': None,
            'account': self.account_name,
            'account_type': AccountType.CREDIT_CARD.value,
            'transaction_type': TransactionType.EXPENSE.value,
            'category': 'Foreign Transaction Fee',
            'amount': amount,
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"{line.strip()} | Related to: {related_txn['notes']}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': source_file.name,
            'data_quality': DataQuality.VERIFIED.value
        }

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the parsed transactions DataFrame."""
        return self._transactions.copy()
