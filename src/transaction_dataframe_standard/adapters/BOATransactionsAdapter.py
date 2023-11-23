import pandas as pd

# TODO:
# class BOATransactionsAdapter:
# class BOADebitTransactionsAdapter:


class BOACreditTransactionsAdapter:
    def __init__(self, filepath):
        # Input
        # Posted Date,Reference Number,Payee,Address,Amount
        # 11/13/2023,24492153316743963851379,"VIATORTRIPADVISOR US 702-749-5744 CA","702-749-5744  CA ",-94.00
        # 11/13/2023,24492153315717648934124,"VISIBLE 866-331-3527 CO","866-331-3527  CO ",-25.00
        # 11/11/2023,31506005710039710988439,"Online payment from CHK 1164","",309.00
        # 11/11/2023,24492153314745557295899,"WINIX INC. 847-551-9900 IL","847-551-9900  IL ",-74.36
        # 11/11/2023,24492153314743498395191,"SPOTIFY 877-778-1161 NY","877-778-1161  NY ",-16.34
        # 10/21/2023,29306005710029852650661,"Online payment from CHK 1164","",720.00

        # Output
        # Date,Time,Type,Name,Category,Description,Money Out,Money In

        self._transactions = pd.read_csv(
            filepath,
            sep=",",
            parse_dates=["Posted Date"],
            dayfirst=True,
        )

        print()
        print(self._transactions.head(10))

        self._transactions.rename(
            columns={"Posted Date": "Date", "Payee": "Description"}, inplace=True
        )
        self._transactions["Time"] = "00:00:00"

        # TODO Need to implement some kind of regex matching to determine Type, Name, and Category
        self._transactions["Type"] = ""
        self._transactions["Name"] = ""
        self._transactions["Category"] = ""

        self._transactions["Money In"] = self._transactions["Amount"].apply(
            lambda x: x if x > 0 else 0
        )
        self._transactions["Money Out"] = self._transactions["Amount"].apply(
            lambda x: -x if x < 0 else 0
        )

        self._transactions.set_index("Date", inplace=True)
        self._transactions.sort_values(by="Date", inplace=True)

        # All these columns irrelevant for the standard.
        self._transactions.drop(
            columns=["Reference Number", "Address", "Amount"],
            inplace=True,
        )
        self._transactions = self._transactions[
            ["Time", "Type", "Name", "Category", "Description", "Money Out", "Money In"]
        ]

        @property
        def transactions(self):
            # space-inefficient but guarantees the internal data is read-only externally
            return self._transactions.copy()
