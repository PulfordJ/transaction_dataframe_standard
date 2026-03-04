"""
Trading212 Transaction Adapter

Parses Trading212 transaction history CSV files and converts them to the
standard transaction format.

Transaction types handled:
- Deposit → Income (deposits from bank)
- Withdrawal → Withdrawal (to other accounts)
- Market buy → Buy (stock purchases with fees)
- Market sell → Sell (stock sales with fees/gains)
- Dividend (Dividend) → Dividend
- Interest on cash → Interest
"""

from pathlib import Path
from typing import Dict, List
import pandas as pd
import numpy as np


class Trading212Adapter:
    """Adapter for Trading212 transaction CSV files"""

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.account_name = "Trading212 ISA"
        self.account_type = "ISA"
        self._transactions = self._parse_csv()

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the processed transactions dataframe (returns a copy)"""
        return self._transactions.copy()

    def _parse_csv(self) -> pd.DataFrame:
        """Parse the CSV file and convert to standard format"""
        df = pd.read_csv(self.csv_path, sep=',', encoding='utf-8')
        df.columns = df.columns.str.strip()

        print(f"Parsing {len(df)} Trading212 transactions...")

        # Parse dates
        df['Time'] = pd.to_datetime(df['Time'], format='%Y-%m-%d %H:%M:%S', errors='coerce')

        # Process each row
        all_transactions = []
        for idx, row in df.iterrows():
            action = str(row['Action']).strip()

            if action == 'Deposit':
                transactions = self._handle_deposit(row)
            elif action == 'Withdrawal':
                transactions = self._handle_withdrawal(row)
            elif action == 'Market buy':
                transactions = self._handle_market_buy(row)
            elif action == 'Market sell':
                transactions = self._handle_market_sell(row)
            elif action == 'Dividend (Dividend)':
                transactions = self._handle_dividend(row)
            elif action == 'Interest on cash':
                transactions = self._handle_interest(row)
            else:
                print(f"  ⚠ Unknown transaction type: {action} on {row['Time']}")
                continue

            all_transactions.extend(transactions)

        # Create DataFrame from transactions
        df_transactions = pd.DataFrame(all_transactions)

        # Set proper data types
        df_transactions['date'] = pd.to_datetime(df_transactions['date'], errors='coerce')
        df_transactions['amount'] = pd.to_numeric(df_transactions['amount'], errors='coerce')
        df_transactions['units'] = pd.to_numeric(df_transactions['units'], errors='coerce')
        df_transactions['price_per_unit'] = pd.to_numeric(df_transactions['price_per_unit'], errors='coerce')
        df_transactions['is_pension_contribution'] = df_transactions['is_pension_contribution'].astype(bool)

        print(f"  ✓ Created {len(df_transactions)} transactions from {len(df)} rows")

        return df_transactions

    def _handle_deposit(self, row) -> List[Dict]:
        """
        Deposit → Income
        Money added to Trading212 account from bank
        """
        date = row['Time']
        amount = float(row['Total'])  # Total column has the deposit amount
        notes = row['Notes'] if pd.notna(row['Notes']) else 'Bank Transfer'

        return [{
            'date': date,
            'time': date.strftime('%H:%M:%S') if pd.notna(date) else None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Income',
            'category': 'Deposit',
            'amount': amount,
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"Trading212 Deposit - {notes}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_withdrawal(self, row) -> List[Dict]:
        """
        Withdrawal → Withdrawal
        Money withdrawn from Trading212 account
        """
        date = row['Time']
        amount = float(row['Total'])  # Negative value

        return [{
            'date': date,
            'time': date.strftime('%H:%M:%S') if pd.notna(date) else None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Withdrawal',
            'category': 'Account Withdrawal',
            'amount': amount,  # Already negative
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"Trading212 Withdrawal - £{abs(amount):,.2f}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_market_buy(self, row) -> List[Dict]:
        """
        Market buy → Buy
        Purchase of stocks/shares
        """
        date = row['Time']
        ticker = row['Ticker'] if pd.notna(row['Ticker']) else 'UNKNOWN'
        name = row['Name'] if pd.notna(row['Name']) else ticker
        shares = float(row['No. of shares'])

        # Price per share is in pence (GBX), need to convert to pounds
        price_pence = float(row['Price / share'])
        price_pounds = price_pence / 100.0

        # Total includes fees
        total = float(row['Total'])

        # Fees
        stamp_duty = float(row['Stamp duty']) if pd.notna(row['Stamp duty']) and row['Stamp duty'] != '' else 0.0
        stamp_duty_reserve = float(row['Stamp duty reserve tax']) if pd.notna(row['Stamp duty reserve tax']) and row['Stamp duty reserve tax'] != '' else 0.0
        ptm_levy = float(row['Ptm levy']) if pd.notna(row['Ptm levy']) and row['Ptm levy'] != '' else 0.0

        total_fees = stamp_duty + stamp_duty_reserve + ptm_levy

        return [{
            'date': date,
            'time': date.strftime('%H:%M:%S') if pd.notna(date) else None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Buy',
            'category': 'Stock Purchase',
            'amount': -total,  # Negative (cash outflow)
            'currency': 'GBP',
            'asset_ticker': ticker,
            'units': shares,
            'price_per_unit': price_pounds,
            'notes': f"Bought {shares:.4f} shares of {name} ({ticker}) at £{price_pounds:.4f}/share (fees: £{total_fees:.2f})",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_market_sell(self, row) -> List[Dict]:
        """
        Market sell → Sell
        Sale of stocks/shares
        """
        date = row['Time']
        ticker = row['Ticker'] if pd.notna(row['Ticker']) else 'UNKNOWN'
        name = row['Name'] if pd.notna(row['Name']) else ticker
        shares = float(row['No. of shares'])

        # Price per share is in pence (GBX), need to convert to pounds
        price_pence = float(row['Price / share'])
        price_pounds = price_pence / 100.0

        # Total is the proceeds received
        total = float(row['Total'])

        # Result column shows capital gain/loss
        result = float(row['Result']) if pd.notna(row['Result']) and row['Result'] != '' else 0.0

        # Ptm levy fee
        ptm_levy = float(row['Ptm levy']) if pd.notna(row['Ptm levy']) and row['Ptm levy'] != '' else 0.0

        return [{
            'date': date,
            'time': date.strftime('%H:%M:%S') if pd.notna(date) else None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Sell',
            'category': 'Stock Sale',
            'amount': total,  # Positive (cash inflow)
            'currency': 'GBP',
            'asset_ticker': ticker,
            'units': shares,
            'price_per_unit': price_pounds,
            'notes': f"Sold {shares:.4f} shares of {name} ({ticker}) at £{price_pounds:.4f}/share (gain: £{result:.2f}, fees: £{ptm_levy:.2f})",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_dividend(self, row) -> List[Dict]:
        """
        Dividend (Dividend) → Dividend
        Dividend payment from holdings
        """
        date = row['Time']
        ticker = row['Ticker'] if pd.notna(row['Ticker']) else 'UNKNOWN'
        name = row['Name'] if pd.notna(row['Name']) else ticker
        shares = float(row['No. of shares']) if pd.notna(row['No. of shares']) else 0.0

        # Dividend per share
        div_per_share_pence = float(row['Price / share'])
        div_per_share_pounds = div_per_share_pence / 100.0

        # Total dividend received
        total = float(row['Total'])

        # Withholding tax
        withholding_tax = float(row['Withholding tax']) if pd.notna(row['Withholding tax']) and row['Withholding tax'] != '' else 0.0

        return [{
            'date': date,
            'time': date.strftime('%H:%M:%S') if pd.notna(date) else None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Dividend',
            'category': 'Dividend Income',
            'amount': total,  # Positive (income)
            'currency': 'GBP',
            'asset_ticker': ticker,
            'units': None,
            'price_per_unit': None,
            'notes': f"Dividend from {name} ({ticker}): {shares:.4f} shares @ £{div_per_share_pounds:.4f}/share (withholding tax: £{withholding_tax:.2f})",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_interest(self, row) -> List[Dict]:
        """
        Interest on cash → Interest
        Interest earned on cash balance
        """
        date = row['Time']
        amount = float(row['Total'])

        return [{
            'date': date,
            'time': date.strftime('%H:%M:%S') if pd.notna(date) else None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Interest',
            'category': 'Interest Income',
            'amount': amount,  # Positive (income)
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': 'Interest on cash balance',
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def get_summary(self) -> Dict:
        """Get summary statistics about parsed transactions"""
        df = self._transactions

        summary = {
            'total_transactions': len(df),
            'date_range': f"{df['date'].min().date()} to {df['date'].max().date()}",
            'transaction_types': df['transaction_type'].value_counts().to_dict(),
            'total_deposits': df[df['transaction_type'] == 'Income']['amount'].sum(),
            'total_withdrawals': df[df['transaction_type'] == 'Withdrawal']['amount'].sum(),
            'total_dividends': df[df['transaction_type'] == 'Dividend']['amount'].sum(),
            'total_interest': df[df['transaction_type'] == 'Interest']['amount'].sum(),
            'assets': df[df['asset_ticker'].notna()]['asset_ticker'].unique().tolist()
        }

        return summary
