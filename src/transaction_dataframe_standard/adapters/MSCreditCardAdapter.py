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
                'Thank you for paying'
            ]):
                in_transaction_section = False
                continue

            if not in_transaction_section:
                continue

            # Skip non-transaction lines
            if any(x in line for x in [
                'M&S points total',
                'see overleaf',
                'Transactions shown on this statement',
                'Balance from previous statement'
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

    # Words that look like capitalized place-names but are actually merchant name
    # fragments, company suffixes, generic nouns, or PDF truncation artifacts.
    # Any city extracted by the regex that matches one of these is discarded.
    _NON_CITY_WORDS = {
        # Company / legal suffixes
        'Ltd', 'Limited', 'Plc', 'Llp', 'Inc', 'Corp', 'Group', 'Gmb',
        'International',
        # Generic business / product words
        'Bus', 'Online', 'Website', 'Membership', 'Purchase', 'Eat', 'Coffee',
        'Food', 'Travel', 'Stores', 'Store', 'Service', 'Services', 'Kiosk',
        'Counter', 'Restaurant', 'Testing', 'Pharmacy', 'Cinemas', 'Cinema',
        'Golf', 'Grill', 'Internet', 'Parking', 'Practice', 'Office',
        'Imaging', 'Science', 'Desk', 'Shop', 'Net', 'Fares', 'Room',
        'Driver', 'Trip', 'Climbing', 'Spa', 'Tax', 'Tel', 'Court',
        'King', 'Bar', 'Garden', 'Rocket', 'Planet', 'Punks', 'Sushi',
        'Street', 'Bird', 'Galore', 'Indigo', 'Apollo', 'Broad', 'Samba',
        'Trails', 'House', 'Greek', 'Truman', 'Sevens',
        # Directional / partial words that bleed from merchant names
        'East', 'West', 'North', 'South', 'Old', 'New',
        # Utility / transport fragments
        'Water', 'Fares',
        # Truncated words from PDF line-wrap
        'Gro', 'Gar', 'Dps', 'Livstreet', 'Leadenha', 'Mchester',
        'Birmingh', 'Restaur', 'Servic', 'Patisseri', 'Climbin',
        # Clearly wrong: country names, partial city fragments, brand noise
        'Uk', 'Limit', 'Marks', 'Wombatscityhostel', 'Beachcomber',
        'Sterling', 'Eagle', 'Los', 'San', 'Resort', 'Maypole', 'Canton',
        'Cherry', 'Bongs', 'Yorke', 'Smith', 'Hounds', 'Spencer', 'Tudge',
        'Cox', 'Limite', 'Membershippat', 'Maxx', 'Water',
    }

    def _parse_merchant_location(self, description: str) -> tuple:
        """
        Parse merchant description to extract clean name, city, and country.

        M&S includes location like "Merchant City Country" or "Merchant City Lnd".

        Examples:
            "Harley Street Dental S London Gbr" -> ("Harley Street Dental S", "London", "UK")
            "Deliveroo London Lnd" -> ("Deliveroo", "London", "UK")
            "Amazon Paris Fra" -> ("Amazon", "Paris", "France")

        Returns:
            (cleaned_name, city, country)
        """
        country_codes = [
            'Gbr', 'Lnd', 'Eng', 'Lux', 'Isr', 'Deu', 'Esp', 'Nld',
            'Ca', 'Irl', 'Usa', 'Fra', 'Bel', 'Che', 'Aut', 'Prt'
        ]
        country_code_map = {
            'gbr': 'UK', 'lnd': 'UK', 'eng': 'UK',
            'fra': 'France', 'deu': 'Germany', 'esp': 'Spain',
            'nld': 'Netherlands', 'irl': 'Ireland', 'usa': 'USA',
            'isr': 'Israel', 'lux': 'Luxembourg', 'bel': 'Belgium',
            'che': 'Switzerland', 'aut': 'Austria', 'prt': 'Portugal',
            'ca': 'Canada',
        }

        country_pattern = '|'.join(country_codes)
        city = None
        country = 'UK'

        # Pattern 1: " City [AreaCode] CountryCode" at end
        pat1 = r'\s+([A-Z][a-z]+)(?:\s+[A-Z][a-z0-9]+)?\s+((?:' + country_pattern + r'))$'
        m1 = re.search(pat1, description, flags=re.IGNORECASE)
        if m1:
            candidate = m1.group(1).strip()
            country = country_code_map.get(m1.group(2).lower(), 'UK')
            cleaned = re.sub(pat1, '', description, flags=re.IGNORECASE).strip()
            # Discard if it's a known non-city word or a short fragment (< 3 chars)
            if len(candidate) >= 3 and candidate not in self._NON_CITY_WORDS:
                city = candidate
        else:
            # Pattern 2: Just " CountryCode" at end (no city)
            pat2 = r'\s+((?:' + country_pattern + r'))$'
            m2 = re.search(pat2, description, flags=re.IGNORECASE)
            if m2:
                country = country_code_map.get(m2.group(1).lower(), 'UK')
            cleaned = re.sub(r'\s+(?:' + country_pattern + r')$', '', description, flags=re.IGNORECASE).strip()

        return cleaned, city, country

    def _extract_payee(self, description: str) -> str:
        """
        Extract and normalize merchant/payee name from description.

        Applies common merchant name mappings for better readability
        and consistency with Monzo transaction naming.

        Returns clean payee name.
        """
        # Common merchant name mappings
        merchant_map = {
            # Transport
            'TFL': 'Transport for London',
            'TFL TRAVEL': 'Transport for London',
            'TRANSPORT FOR LONDON': 'Transport for London',
            ')))TFL': 'Transport for London',
            'UBER': 'Uber',
            'NATIONAL RAIL': 'National Rail',

            # Healthcare
            'HARLEY STREET DENTAL': 'Harley Street Dental S',

            # Booking services
            'BKNG.COM': 'Booking.com',
            'BKG*': 'Booking.com',
            'BOOKING.COM': 'Booking.com',
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
            'GREGGS': 'Greggs',

            # Retail
            'AMAZON': 'Amazon',
            'TESCO': 'Tesco',
            'SAINSBURYS': "Sainsbury's",
            'ASDA': 'Asda',
            'MORRISONS': 'Morrisons',
            'WAITROSE': 'Waitrose',
            'MARKS & SPENCER': 'Marks & Spencer',
            'M&S': 'Marks & Spencer',
            'LIDL': 'Lidl',
            'ALDI': 'Aldi',

            # Online services
            'PAYPAL': 'PayPal',
            'GOOGLE': 'Google',
            'APPLE.COM': 'Apple',
            'MICROSOFT': 'Microsoft',
            'NETFLIX': 'Netflix',
            'SPOTIFY': 'Spotify',
            'AMAZON PRIME': 'Amazon Prime',

            # Delivery
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

        # Remove leading parentheses/symbols (e.g., ")))Tfl" -> "Tfl")
        payee = re.sub(r'^[)]+', '', payee)

        # Remove trailing location/codes
        parts = payee.split()
        if len(parts) > 1:
            # Keep first 2-3 words as the payee
            # Skip if last word looks like a location code or URL
            if len(parts[-1]) <= 4 and parts[-1].isupper():
                payee = ' '.join(parts[:-1])
            # Remove URLs/domains
            elif any(x in parts[-1].lower() for x in ['.com', '.co.uk', '.gov.uk', 'http']):
                payee = ' '.join(parts[:-1])

        # Remove trailing dashes, slashes, and spaces
        payee = payee.rstrip(' -/')

        # Title case for better readability (unless all caps suggests acronym)
        if len(payee) > 4 and payee.isupper() and ' ' in payee:
            payee = payee.title()

        return payee if payee else description

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
        # Note: Some lines have extra text after the amount (e.g., "£486.26CR M&S"), so we don't require end-of-line
        pattern = r'^(\d{2}\s+\w{3}\s+\d{2})\s+(\d{2}\s+\w{3}\s+\d{2})\s+(.+?)\s+(£[\d,]+\.\d{2}(?:\s*CR)?)'

        match = re.match(pattern, line)
        if not match:
            return None

        date_applied_str = match.group(1)
        date_transaction_str = match.group(2)
        description = match.group(3).strip()
        amount_str = match.group(4)

        # Parse location from description (extracts city/country and cleans name)
        description, city, country = self._parse_merchant_location(description)

        # Extract normalized payee name
        payee = self._extract_payee(description)

        # Parse date (use Date of Transaction, not Date Applied)
        try:
            date = datetime.strptime(date_transaction_str, '%d %b %y').date()
        except:
            return None

        # Parse amount
        amount, is_credit = self._parse_amount(amount_str)

        # Classify transaction
        txn_type, category = self._classify_transaction(description, is_credit, amount)

        # Build notes field with structured format (similar to Monzo/Halifax)
        notes = f"Payee: {payee}"
        if description != payee:
            notes += f" | Description: {description}"
        notes += f" | Date Applied: {date_applied_str}"

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
            'notes': notes,
            'country': country,
            'city': city,
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
