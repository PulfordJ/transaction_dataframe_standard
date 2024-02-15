from datetime import datetime

import pandas as pd

# Better settings to debug a dataframe via printouts
from transaction_dataframe_standard.functions import print_expense_stats_trailing_twelve_months

pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)


def test_expense_function():
    transactions = pd.read_csv("input/StandardDataExpected.csv", sep=',', parse_dates=["Date"], index_col="Date")

    # TODO build test to ensure excluded_names are removed from data frame.
    excluded_names = {"Vanguard", "Hargreaves Lansdown", "TradingView", "HMRC"}
    # TODO actually break this down into testable outputs, right now I'm just testing that it doesn't fail to execute.
    print_expense_stats_trailing_twelve_months(transactions, excluded_names, datetime(2020, 10, 28), datetime(2019, 10, 28))