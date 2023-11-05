import pandas as pd
from pandas.testing import assert_frame_equal
from transaction_dataframe_standard.adapters.MonzoTransactionsAdapter import MonzoTransactionsAdapter

# Better settings to debug a dataframe via printouts
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.expand_frame_repr', False)


def test_adapter():
    # TODO a function like build_and_test_adapter(AdapterClassName, inputfilename, expectedfilename) should exist to
    #  test muliple adapters in a parameterized way
    monzo_transactions_adapter = MonzoTransactionsAdapter("input/MonzoDataInput.csv")
    transactions = monzo_transactions_adapter._transactions
    print()
    print(transactions.head())

    expected_transactions = pd.read_csv("input/StandardDataExpected.csv", sep=',', parse_dates=["Date"], dayfirst=True,
                                        index_col="Date")
    assert_frame_equal(transactions, expected_transactions)
