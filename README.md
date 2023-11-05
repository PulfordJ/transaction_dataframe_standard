The best way to get started is to take a look at the tests folder, which provide up to date examples on how to use this library. The goal of this project is to standardise cash bank account transaction dataframes for the purposes of processing in Pandas. 

With that goal in mind this project has two halves, one half provides adapters. For example the CSV data [here](test/input/MonzoDataInput.csv) is similar to a real Monzo CSV output. The MonzoTransactionsAdapter turns that data into the standardized data, one example can be found  [here](test/input/StandardDataExpected.csv)

On which any functions in the functions package can be applied!
