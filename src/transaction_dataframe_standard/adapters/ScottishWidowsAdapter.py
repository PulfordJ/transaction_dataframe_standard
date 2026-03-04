"""
ScottishWidows Pension Transaction Adapter

Parses ScottishWidows pension unit movement history CSV files and converts them
to the standard transaction format.

Transaction types handled:
- Normal Premium / Single Premium → Split into Income + Buy
- AMC Adjustment → Fee (management charges)
- Switch Buy / Switch Sell → Buy / Sell
- SW Admin Charge → Fee (or Withdrawal for final exit)
"""

from pathlib import Path
from typing import Dict, List
import pandas as pd
import numpy as np


# Fund ticker mappings (ScottishWidows funds not available on yfinance)
FUND_TICKER_MAP = {
    'SW Pension Portfolio Two Series 2': 'SW-PP2-S2',
    'SW Pension Portfolio One Series 2': 'SW-PP1-S2',
    'Scottish Widows Pension Portfolio Two CS8': 'SW-PP2-CS8',
    'Scottish Widows Pension Portfolio One CS8': 'SW-PP1-CS8',
}


class ScottishWidowsAdapter:
    """Adapter for ScottishWidows pension transaction CSV files"""

    def __init__(self, csv_path: str):
        self.csv_path = Path(csv_path)
        self.account_name = "Scottish Widows Pension"
        self.account_type = "SIPP"
        self._transactions = self._parse_csv()

    @property
    def transactions(self) -> pd.DataFrame:
        """Get the processed transactions dataframe (returns a copy)"""
        return self._transactions.copy()

    def _parse_csv(self) -> pd.DataFrame:
        """Parse the CSV file and convert to standard format"""
        # Read CSV with appropriate encoding for pound sign (£)
        df = pd.read_csv(self.csv_path, sep=',', encoding='ISO-8859-1')
        df.columns = df.columns.str.strip()

        # Clean up column names (remove BOM if present)
        if 'VALUE (�)' in df.columns:
            df.rename(columns={'VALUE (�)': 'VALUE (£)'}, inplace=True)

        print(f"Parsing {len(df)} ScottishWidows transactions...")

        # Parse dates
        df['DATE'] = pd.to_datetime(df['DATE'], dayfirst=True, errors='coerce')

        # Process each row
        all_transactions = []
        for idx, row in df.iterrows():
            txn_type = str(row['TYPE']).strip()

            if 'Premium' in txn_type:
                # Normal Premium or Single Premium → Income + Buy
                transactions = self._handle_premium(row)
            elif txn_type == 'AMC Adjustment':
                transactions = self._handle_amc_adjustment(row)
            elif txn_type in ['Switch Buy', 'Switch buy']:
                transactions = self._handle_switch_buy(row)
            elif txn_type in ['Switch Sell', 'Switch sell']:
                transactions = self._handle_switch_sell(row)
            elif txn_type == 'SW Admin Charge':
                transactions = self._handle_sw_admin_charge(row)
            else:
                print(f"  ⚠ Unknown transaction type: {txn_type} on {row['DATE']}")
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

    def _handle_premium(self, row) -> List[Dict]:
        """
        Split premium into Income + Buy transactions
        Premiums are pension contributions that are immediately invested
        """
        date = row['DATE']
        # Remove commas from numeric values before converting
        amount = float(str(row['VALUE (£)']).replace(',', ''))
        units = float(str(row['UNITS']).replace(',', ''))
        bid_price_pence = float(str(row['BID PRICE (p)']).replace(',', ''))
        price_per_unit = bid_price_pence / 100  # Convert pence to pounds
        fund = str(row['FUNDS']).strip()
        ticker = FUND_TICKER_MAP.get(fund, f"SW-{fund[:20].replace(' ', '-')}")
        txn_type = row['TYPE']

        transactions = []

        # 1. Income transaction (pension contribution received)
        income_txn = {
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Income',
            'category': 'Pension Contribution',
            'amount': amount,  # Positive
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"{txn_type} - {fund}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': True,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }
        transactions.append(income_txn)

        # 2. Buy transaction (fund purchase with that contribution)
        buy_txn = {
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Buy',
            'category': 'Fund Purchase',
            'amount': -amount,  # Negative (cash outflow)
            'currency': 'GBP',
            'asset_ticker': ticker,
            'units': units,
            'price_per_unit': price_per_unit,
            'notes': f"Bought {units:.2f} units of {fund} at £{price_per_unit:.4f}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }
        transactions.append(buy_txn)

        return transactions

    def _handle_amc_adjustment(self, row) -> List[Dict]:
        """
        AMC Adjustment → Fee (Annual Management Charge)
        These are expenses deducted from the fund
        """
        date = row['DATE']
        amount = float(str(row['VALUE (£)']).replace(',', ''))
        fund = str(row['FUNDS']).strip()

        return [{
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Fee',
            'category': 'Annual Management Charge',
            'amount': amount,  # Should be positive (represents cost)
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"AMC Adjustment - {fund}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_switch_buy(self, row) -> List[Dict]:
        """
        Switch Buy → Buy
        Buying units in a new fund during fund reallocation
        """
        date = row['DATE']
        amount = float(str(row['VALUE (£)']).replace(',', ''))
        units = float(str(row['UNITS']).replace(',', ''))
        bid_price_pence = float(str(row['BID PRICE (p)']).replace(',', ''))
        price_per_unit = bid_price_pence / 100
        fund = str(row['FUNDS']).strip()
        ticker = FUND_TICKER_MAP.get(fund, f"SW-{fund[:20].replace(' ', '-')}")

        return [{
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Buy',
            'category': 'Fund Switch',
            'amount': -amount,  # Negative (cash outflow, though it's from switch proceeds)
            'currency': 'GBP',
            'asset_ticker': ticker,
            'units': units,
            'price_per_unit': price_per_unit,
            'notes': f"Switch Buy: {units:.2f} units of {fund} at £{price_per_unit:.4f}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_switch_sell(self, row) -> List[Dict]:
        """
        Switch Sell → Sell
        Selling units from an old fund during fund reallocation
        """
        date = row['DATE']
        amount = float(str(row['VALUE (£)']).replace(',', ''))
        units = float(str(row['UNITS']).replace(',', ''))
        bid_price_pence = float(str(row['BID PRICE (p)']).replace(',', ''))
        price_per_unit = bid_price_pence / 100
        fund = str(row['FUNDS']).strip()
        ticker = FUND_TICKER_MAP.get(fund, f"SW-{fund[:20].replace(' ', '-')}")

        return [{
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Sell',
            'category': 'Fund Switch',
            'amount': amount,  # Positive (cash inflow from sale)
            'currency': 'GBP',
            'asset_ticker': ticker,
            'units': abs(units),  # Units should be positive for Sell
            'price_per_unit': price_per_unit,
            'notes': f"Switch Sell: {abs(units):.2f} units of {fund} at £{price_per_unit:.4f}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def _handle_sw_admin_charge(self, row) -> List[Dict]:
        """
        SW Admin Charge → Usually Fee, but final exit is Withdrawal
        The final £177k charge represents the pension transfer out to Halifax SIPP
        For final exit, need to sell all units first, then withdraw cash
        """
        date = row['DATE']
        amount = float(str(row['VALUE (£)']).replace(',', ''))
        fund = str(row['FUNDS']).strip()
        ticker = FUND_TICKER_MAP.get(fund, f"SW-{fund[:20].replace(' ', '-')}")

        # Special case: final exit transfer (2023-09-07, ~£177k)
        # This has units that need to be sold before withdrawal
        if date.year == 2023 and date.month == 9 and abs(amount) > 100000:
            units = float(str(row['UNITS']).replace(',', ''))
            bid_price_pence = float(str(row['BID PRICE (p)']).replace(',', ''))
            price_per_unit = bid_price_pence / 100

            transactions = []

            # 1. Sell transaction (liquidate all holdings)
            sell_txn = {
                'date': date,
                'time': None,
                'account': self.account_name,
                'account_type': self.account_type,
                'transaction_type': 'Sell',
                'category': 'Pension Exit - Fund Liquidation',
                'amount': abs(amount),  # Positive (cash inflow from sale)
                'currency': 'GBP',
                'asset_ticker': ticker,
                'units': abs(units),  # Positive units sold
                'price_per_unit': price_per_unit,
                'notes': f"SW Exit Sale: {abs(units):,.2f} units of {fund} at £{price_per_unit:.4f}",
                'country': 'UK',
                'city': None,
                'is_pension_contribution': False,
                'data_source': self.csv_path.name,
                'data_quality': 'Verified'
            }
            transactions.append(sell_txn)

            # 2. Withdrawal transaction (transfer cash out)
            withdrawal_txn = {
                'date': date,
                'time': None,
                'account': self.account_name,
                'account_type': self.account_type,
                'transaction_type': 'Withdrawal',
                'category': 'Pension Transfer Out',
                'amount': amount,  # Negative (outflow)
                'currency': 'GBP',
                'asset_ticker': None,
                'units': None,
                'price_per_unit': None,
                'notes': f"SW Exit - Transfer to Halifax SIPP (£177,323.13 received on 2023-09-22, £12 fee difference)",
                'country': 'UK',
                'city': None,
                'is_pension_contribution': False,
                'data_source': self.csv_path.name,
                'data_quality': 'Verified'
            }
            transactions.append(withdrawal_txn)

            return transactions

        # Normal admin charge - fee
        return [{
            'date': date,
            'time': None,
            'account': self.account_name,
            'account_type': self.account_type,
            'transaction_type': 'Fee',
            'category': 'Administrative Fee',
            'amount': abs(amount),  # Make positive (represents cost)
            'currency': 'GBP',
            'asset_ticker': None,
            'units': None,
            'price_per_unit': None,
            'notes': f"SW Admin Charge - {fund}",
            'country': 'UK',
            'city': None,
            'is_pension_contribution': False,
            'data_source': self.csv_path.name,
            'data_quality': 'Verified'
        }]

    def verify_prices(self) -> List[Dict]:
        """
        Verify price consistency: amount ≈ units × price
        Returns list of any discrepancies found
        """
        df = pd.read_csv(self.csv_path, sep=',', encoding='ISO-8859-1')
        df.columns = df.columns.str.strip()

        if 'VALUE (�)' in df.columns:
            df.rename(columns={'VALUE (�)': 'VALUE (£)'}, inplace=True)

        issues = []
        for idx, row in df.iterrows():
            try:
                units = float(row['UNITS'])
                price_pence = float(row['BID PRICE (p)'])
                amount = float(row['VALUE (£)'])

                if units and price_pence and amount:
                    expected_amount = units * (price_pence / 100)
                    diff = abs(expected_amount - abs(amount))

                    if diff > 0.10:  # More than 10p difference
                        issues.append({
                            'date': row['DATE'],
                            'fund': row['FUNDS'],
                            'type': row['TYPE'],
                            'expected': expected_amount,
                            'actual': amount,
                            'diff': diff
                        })
            except (ValueError, TypeError):
                continue

        return issues

    def get_summary(self) -> Dict:
        """Get summary statistics about parsed transactions"""
        df = self._transactions

        summary = {
            'total_transactions': len(df),
            'date_range': f"{df['date'].min().date()} to {df['date'].max().date()}",
            'transaction_types': df['transaction_type'].value_counts().to_dict(),
            'total_income': df[df['transaction_type'] == 'Income']['amount'].sum(),
            'total_purchases': abs(df[df['transaction_type'] == 'Buy']['amount'].sum()),
            'total_fees': df[df['transaction_type'] == 'Fee']['amount'].sum(),
            'funds': df[df['asset_ticker'].notna()]['asset_ticker'].unique().tolist()
        }

        return summary
