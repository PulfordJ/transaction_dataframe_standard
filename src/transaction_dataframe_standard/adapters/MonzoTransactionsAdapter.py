import pandas as pd

class MonzoTransactionsAdapter:
    def __init__(self, filepath):
        # TODO Potentially extract the logic that finds the CSV file so that specifying a folder as input results in something like
        # parent_dir = "./input"
        # all_files = glob.glob(os.path.join(parent_dir, "*.csv"))
        # all_files = glob.glob(os.path.join(filepath))
        # self._transactions = pd.concat(
        #     (pd.read_csv(f, sep=',', parse_dates=["Date"], dayfirst=True, index_col="Date") for f in all_files))

        self._transactions = pd.read_csv(filepath, sep=',', parse_dates=["Date"], dayfirst=True, index_col="Date")

        print()
        print(self._transactions)

        self._transactions.sort_values(by="Date", inplace=True)

        # All these columns irrelevant for the standard.
        self._transactions.drop(
            columns=["Emoji", "Transaction ID", "Address", "Local currency", "Currency", "Category split",
                     "Notes and #tags",
                     "Receipt", "Local amount", "Amount"], inplace=True)

        @property
        def transactions(self):
            # space-inefficient but guarantees the internal data is read-only externally
            return self._transactions.copy()
