The goal of this project is to standardise cash bank account transaction dataframes for the purposes of processing in Pandas. 

With that goal in mind this project has two halfs, one half provides adapters. For example the below CSV data is similar to a real Monzo CSV output:


Transaction ID      Time                  Type                 Name Emoji Category   Amount Currency  Local amount Local currency Notes and #tags  Address  Receipt                               Description  Category split  Money Out  Money In
Date                                                                                                                                                                                                                                                                  
2020-08-24   tx_0000TwgfJoVst7ORW5  15:40:42  Bacs (Direct Credit)   Employment Example   NaN   Income  2000.28      GBP       2000.28            GBP      MONZO BANK      NaN      NaN                                MONZO BANK             NaN        NaN   2000.28
2020-08-25   tx_0000TwgfJoVst7ORW5  15:40:42  Bacs (Direct Credit)   Employment Example   NaN   Income  2000.28      GBP       2000.28            GBP      MONZO BANK      NaN      NaN                                MONZO BANK             NaN        NaN   2000.28
2020-09-25    tx_00003QvviyBMCQatl  16:10:45        Faster payment            HELPTOBUY   NaN  General  -136.86      GBP       -136.86            GBP       HELPTOBUY      NaN      NaN                                 HELPTOBUY             NaN    -136.86       NaN
2020-10-03  tx_0000PpkTbQcjCMuOy2N  09:06:17          Card payment  Amazon Web Services    ☁️    Bills    -0.93      GBP         -1.20            USD             NaN      NaN      NaN  AWS EMEA               aws.amazon.co LUX             NaN      -0.93       NaN



The MonzoTransactionsAdapter turns that data into the standardized data below:
                Time                  Type                 Name Category                               Description  Money Out  Money In
Date                                                                                                                                   
2020-08-24  15:40:42  Bacs (Direct Credit)   Employment Example   Income                                MONZO BANK        NaN   2000.28
2020-08-25  15:40:42  Bacs (Direct Credit)   Employment Example   Income                                MONZO BANK        NaN   2000.28
2020-09-25  16:10:45        Faster payment            HELPTOBUY  General                                 HELPTOBUY    -136.86       NaN
2020-10-03  09:06:17          Card payment  Amazon Web Services    Bills  AWS EMEA               aws.amazon.co LUX      -0.93       NaN

On which any functions in the functions package can be applied!