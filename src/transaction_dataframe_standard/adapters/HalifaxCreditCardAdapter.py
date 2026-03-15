"""
Halifax Credit Card Adapter

Parses Halifax Clarity credit card statement PDFs which include:
- Purchases (negative amounts): Store purchases, online transactions
- Payment receipts (positive amounts with CR): Direct Debit, manual payments
- Fees: Foreign transaction fees, late payment fees, interest charges
- Foreign currency conversions
- Running balance

Account: Halifax Clarity (5253 03** **** 0356)
Account Type: Credit Card
"""

import pandas as pd
import pdfplumber
import re
from pathlib import Path
from typing import List, Tuple, Optional
from datetime import datetime, date

from ..standard import (
    TransactionType, AccountType, DataQuality,
    create_empty_standard_dataframe
)


class HalifaxCreditCardAdapter:
    """
    Adapter for Halifax Clarity Credit Card statement PDFs.

    Extracts transaction data from monthly PDF statements including
    purchases, payments, fees, and foreign currency transactions.

    Key difference from M&S: Halifax dates don't include year, must infer from statement date.
    """

    def __init__(self, pdf_paths, account_name: str = "Halifax Clarity"):
        """
        Initialize Halifax Credit Card adapter.

        Args:
            pdf_paths: List of paths to Halifax statement PDFs (or single path)
            account_name: Name for the account (default: "Halifax Clarity")
        """
        # Handle single path or list of paths
        if isinstance(pdf_paths, str):
            pdf_paths = [pdf_paths]

        self.pdf_paths = [Path(p) for p in pdf_paths]
        self.account_name = account_name
        self.account_number = "5253 03** **** 0356"

        # Parse all PDFs
        self._transactions = self._parse_all_pdfs()

    def _parse_all_pdfs(self) -> pd.DataFrame:
        """Parse all PDF statements and combine into single DataFrame."""
        all_transactions = []

        print(f"Parsing {len(self.pdf_paths)} Halifax Credit Card statements...")

        for pdf_path in sorted(self.pdf_paths):
            try:
                transactions = self._parse_single_pdf(pdf_path)
                all_transactions.extend(transactions)
                print(f"  ✓ {pdf_path.name}: {len(transactions)} transactions")
            except Exception as e:
                print(f"  ✗ Error parsing {pdf_path.name}: {e}")
                import traceback
                traceback.print_exc()
                continue

        if not all_transactions:
            return create_empty_standard_dataframe()

        df = pd.DataFrame(all_transactions)
        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values('date').reset_index(drop=True)

        print(f"  ✓ Total: {len(df)} Halifax transactions")
        return df

    def _parse_single_pdf(self, pdf_path: Path) -> List[dict]:
        """Parse a single Halifax PDF statement."""
        transactions = []

        with pdfplumber.open(pdf_path) as pdf:
            # Extract statement date from first page
            first_page_text = pdf.pages[0].extract_text()
            statement_date = self._extract_statement_date(first_page_text)

            if not statement_date:
                print(f"  ⚠ Could not extract statement date from {pdf_path.name}, skipping")
                return []

            # Parse all pages for transactions
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                page_transactions = self._parse_page_text(text, statement_date, pdf_path)
                transactions.extend(page_transactions)

        return transactions

    def _extract_statement_date(self, first_page_text: str) -> Optional[date]:
        """
        Extract statement date from first page.

        Looking for patterns like:
        - "21 May 2023"
        - "Statement date 21 May 2023"
        """
        # Pattern: DD Month YYYY or DD MMM YYYY
        pattern = r'(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})'

        match = re.search(pattern, first_page_text, re.IGNORECASE)
        if match:
            day = int(match.group(1))
            month_str = match.group(2)
            year = int(match.group(3))

            try:
                # Parse the date
                parsed_date = datetime.strptime(f"{day} {month_str} {year}", '%d %B %Y').date()
                return parsed_date
            except ValueError:
                try:
                    # Try abbreviated month
                    parsed_date = datetime.strptime(f"{day} {month_str} {year}", '%d %b %Y').date()
                    return parsed_date
                except ValueError:
                    return None

        return None

    def _parse_page_text(self, text: str, statement_date: date, source_file: Path) -> List[dict]:
        """Parse transaction lines from page text."""
        transactions = []
        lines = text.split('\n')

        # Find transaction section
        in_transaction_section = False
        prev_transaction = None

        for line in lines:
            # Start of transaction section
            if 'Date of transaction' in line and 'Date entered' in line:
                in_transaction_section = True
                continue

            # End of transaction section
            if in_transaction_section and any(x in line for x in [
                'Page', 'Total purchases', 'Total payments',
                'New balance', 'Minimum payment', 'Payment due date',
                'Credit limit', 'Available credit'
            ]):
                in_transaction_section = False
                continue

            if not in_transaction_section:
                continue

            # Skip non-transaction lines
            if any(x in line for x in [
                'BALANCE FROM PREVIOUS STATEMENT',
                'Balance from previous statement',
                'Brought forward'
            ]):
                continue

            # Try to parse as transaction
            txn = self._parse_transaction_line(line, statement_date, source_file)
            if txn:
                transactions.append(txn)
                prev_transaction = txn
            elif prev_transaction:
                # Check if this is a continuation line (FX conversion)
                if '@' in line and not any(x in line for x in ['Date of', 'Exchange Rate']):
                    # Foreign currency detail: "35.00 ILS @ 4.4872"
                    prev_transaction['notes'] += f" | FX: {line.strip()}"
                elif 'Exchange Rate' in line:
                    # Exchange rate note
                    prev_transaction['notes'] += f" | {line.strip()}"

        return transactions

    def _infer_transaction_year(self, txn_month_str: str, statement_date: date) -> int:
        """
        Infer transaction year from statement date.

        Example: Statement dated Jan 2024
        - Transaction in Dec → 2023
        - Transaction in Jan → 2024
        """
        month_map = {
            'JANUARY': 1, 'FEBRUARY': 2, 'MARCH': 3, 'APRIL': 4,
            'MAY': 5, 'JUNE': 6, 'JULY': 7, 'AUGUST': 8,
            'SEPTEMBER': 9, 'OCTOBER': 10, 'NOVEMBER': 11, 'DECEMBER': 12,
            'JAN': 1, 'FEB': 2, 'MAR': 3, 'APR': 4,
            'JUN': 6, 'JUL': 7, 'AUG': 8, 'SEP': 9,
            'OCT': 10, 'NOV': 11, 'DEC': 12
        }

        txn_month_num = month_map.get(txn_month_str.upper())
        if txn_month_num is None:
            return statement_date.year

        statement_month = statement_date.month

        # If transaction month is Dec and statement month is Jan/Feb, use previous year
        if txn_month_num == 12 and statement_month <= 2:
            return statement_date.year - 1
        # If transaction month > statement month by more than 6, it's from previous year
        elif txn_month_num > statement_month and (txn_month_num - statement_month) > 6:
            return statement_date.year - 1
        else:
            return statement_date.year

    def _clean_merchant_name(self, description: str) -> str:
        """
        Clean merchant name by removing location suffixes.

        Halifax includes location like "Merchant City Country" or "Merchant City ISR"
        This removes the location suffix to match Monzo's cleaner format.

        Examples:
            "ISRAELI RAILWAYS HASHA TEL AVIV ISR" -> "ISRAELI RAILWAYS HASHA"
            "PIZZA HUT JERUSALEM ISR" -> "PIZZA HUT"
            "SUPER PHARM JERUSALEM ISR" -> "SUPER PHARM"
        """
        # Country codes commonly seen in Halifax statements
        country_codes = [
            'Gbr', 'Lnd', 'Eng', 'Lux', 'Isr', 'Deu', 'Esp', 'Nld',
            'Ca', 'Irl', 'Usa', 'Fra', 'Bel', 'Che', 'Aut', 'Prt', 'GBR', 'ISR'
        ]

        # Build regex pattern to match " City CountryCode" at end
        # Handles multi-part cities like "TEL AVIV ISR"
        country_pattern = '|'.join(country_codes)

        # Pattern 1: " City [AreaCode] CountryCode" at end
        # Matches: " TEL AVIV ISR", " JERUSALEM ISR", " LONDON GBR", etc.
        pattern = r'\s+[A-Z][A-Za-z]+(?:\s+[A-Z][A-Za-z0-9]+)?\s+(?:' + country_pattern + r')$'
        cleaned = re.sub(pattern, '', description, flags=re.IGNORECASE)

        # Pattern 2: Just " CountryCode" at end (for cases without city)
        pattern2 = r'\s+(?:' + country_pattern + r')$'
        cleaned = re.sub(pattern2, '', cleaned, flags=re.IGNORECASE)

        return cleaned.strip()

    def _extract_payee(self, description: str) -> str:
        """
        Extract and normalize merchant/payee name from description.

        Applies common merchant name mappings for better readability:
        - BKNG.COM / BOOKING.COM → Booking.com
        - MCDONALDS → McDonald's
        - etc.

        Returns clean payee name.
        """
        # Common merchant name mappings
        merchant_map = {
            # Transport
            'TFL': 'Transport for London',
            'TFL TRAVEL': 'Transport for London',
            'TRANSPORT FOR LONDON': 'Transport for London',
            'UBER': 'Uber',
            'NATIONAL RAIL': 'National Rail',

            # Healthcare
            'HARLEY STREET DENTAL': 'Harley Street Dental S',

            # Booking services
            'BKNG.COM': 'Booking.com',
            'BKG*': 'Booking.com',
            'BOOKING.COM': 'Booking.com',
            'BOOKING COM': 'Booking.com',
            'BOOKING.': 'Booking.com',
            'AIRBNB': 'Airbnb',

            # Food chains
            'MCDONALDS': "McDonald's",
            'BURGER KING': 'Burger King',
            'KFC': 'KFC',
            'PIZZA HUT': 'Pizza Hut',
            'DOMINOS': "Domino's",
            'SUBWAY': 'Subway',
            'STARBUCKS': 'Starbucks',
            'COSTA COFFEE': 'Costa Coffee',
            'PRET A MANGER': 'Pret A Manger',

            # Transport
            'ISRAELI RAILWAYS': 'Israeli Railways',
            'UBER': 'Uber',
            'LYFT': 'Lyft',
            'NATIONAL RAIL': 'National Rail',

            # Retail
            'AMAZON': 'Amazon',
            'TESCO': 'Tesco',
            'SAINSBURYS': "Sainsbury's",
            'ASDA': 'Asda',
            'MORRISONS': 'Morrisons',
            'WAITROSE': 'Waitrose',
            'MARKS & SPENCER': 'Marks & Spencer',
            'M&S': 'Marks & Spencer',

            # Online services
            'PAYPAL': 'PayPal',
            'GOOGLE': 'Google',
            'APPLE.COM': 'Apple',
            'MICROSOFT': 'Microsoft',
            'NETFLIX': 'Netflix',
            'SPOTIFY': 'Spotify',
            'AMAZON PRIME': 'Amazon Prime',

            # Other
            'DELIVEROO': 'Deliveroo',
            'JUST EAT': 'Just Eat',
            'UBER EATS': 'Uber Eats',
        }

        # Try exact match first
        desc_upper = description.upper()
        for pattern, normalized in merchant_map.items():
            if pattern.upper() in desc_upper:
                return normalized

        # If no mapping found, extract first meaningful part
        # Remove common suffixes/prefixes
        payee = description.strip()

        # Remove trailing location/codes (e.g., "LONDON", "TEL AVIV")
        # This catches any remaining location info after _clean_merchant_name
        parts = payee.split()
        if len(parts) > 1:
            # Keep first 2-3 words as the payee (most merchants are 1-3 words)
            # Skip if last word looks like a location code
            if len(parts[-1]) <= 4 and parts[-1].isupper():
                payee = ' '.join(parts[:-1])

        # Remove trailing dashes and spaces
        payee = payee.rstrip(' -')

        # Title case for better readability (unless all caps suggests acronym)
        if len(payee) > 4 and payee.isupper() and ' ' in payee:
            payee = payee.title()

        return payee if payee else description

    def _parse_transaction_line(self, line: str, statement_date: date, source_file: Path) -> Optional[dict]:
        """
        Parse a single transaction line.

        Format: "DD MONTH DD MONTH Description Amount"
        Example: "27 APRIL 01 MAY ISRAELI RAILWAYS HASHA TEL AVIV ISR 7.80"
        Example with CR: "18 MAY 18 MAY DIRECT DEBIT PAYMENT - THANK YOU 39.95CR"
        """
        # Regex pattern for Halifax transaction line
        # Group 1: Date of Transaction (DD MONTH)
        # Group 2: Date Entered (DD MONTH)
        # Group 3: Description
        # Group 4: Amount (X.XX or X.XXCR)
        pattern = r'^(\d{1,2}\s+[A-Z]+)\s+(\d{1,2}\s+[A-Z]+)\s+(.+?)\s+([\d,]+\.\d{2}(?:CR)?)\s*$'

        match = re.match(pattern, line, re.IGNORECASE)
        if not match:
            return None

        date_transaction_str = match.group(1)
        date_entered_str = match.group(2)
        description = match.group(3).strip()
        amount_str = match.group(4)

        # Clean merchant name (remove location suffixes like "TEL AVIV ISR")
        description = self._clean_merchant_name(description)

        # Extract normalized payee name
        payee = self._extract_payee(description)

        # Parse date (use Date of Transaction, not Date Entered)
        try:
            # Extract day and month
            parts = date_transaction_str.split()
            if len(parts) != 2:
                return None

            day = int(parts[0])
            month_str = parts[1]

            # Infer year from statement date
            year = self._infer_transaction_year(month_str, statement_date)

            # Construct full date string and parse
            date_str = f"{day} {month_str} {year}"
            txn_date = datetime.strptime(date_str, '%d %B %Y').date()
        except:
            try:
                # Try abbreviated month format
                txn_date = datetime.strptime(date_str, '%d %b %Y').date()
            except:
                return None

        # Parse amount
        amount, is_credit = self._parse_amount(amount_str)

        # Classify transaction
        txn_type, category = self._classify_transaction(description, is_credit, amount)

        # Build notes field with structured format (similar to Monzo)
        notes = f"Payee: {payee}"
        if description != payee:
            notes += f" | Description: {description}"
        notes += f" | Date Entered: {date_entered_str}"

        return {
            'date': txn_date,
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
            'notes': notes,
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': source_file.name,
            'data_quality': DataQuality.VERIFIED.value
        }

    def _parse_amount(self, amount_str: str) -> Tuple[float, bool]:
        """Parse amount and determine if it's a credit (payment receipt)."""
        is_credit = 'CR' in amount_str.upper()
        clean = amount_str.replace('CR', '').replace('cr', '').replace(',', '').strip()
        amount = float(clean)

        # Apply correct sign:
        # - Credits (payments IN to Halifax) are POSITIVE (reduce debt)
        # - Debits (purchases) are NEGATIVE (increase debt)
        return (amount if is_credit else -amount, is_credit)

    def _classify_transaction(self, description: str, is_credit: bool, amount: float) -> Tuple[str, str]:
        """
        Classify Halifax transaction type and category.

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
                if 'foreign' in desc_lower or 'non-sterling' in desc_lower:
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

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the parsed transactions DataFrame."""
        return self._transactions.copy()
