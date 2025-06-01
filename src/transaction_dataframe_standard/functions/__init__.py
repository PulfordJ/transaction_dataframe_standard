from datetime import datetime

from dateutil.relativedelta import relativedelta
import pandas as pd

# TODO maybe remove this function and stub datetime.today instead
def print_expense_stats_trailing_twelve_months(transactions, excluded_names, last_day, first_day):
    transactions_minus_investments = transactions.loc[
        ~(transactions["Name"].isin(excluded_names))]

    # Define a search function
    def search_string(s, search):
        return search in str(s).lower()

    # TODO move this to a utility page, for debugging future functions.
    def find_text_in_df(df, text):
        # example: find_text_in_df(df_monzo_transactions_minus_investments, "monument")
        # Search for the string 'al' in all columns
        mask = df.applymap(lambda x: search_string(x, text))

        # Filter the DataFrame based on the mask
        filtered_df = df.loc[mask.any(axis=1)]
        print(filtered_df.head())

    def get_date_string(my_date, ):
        return my_date.strftime('%Y-%m-%d')

    daily_expenses = transactions_minus_investments.loc[
        transactions_minus_investments["Money Out"].notna()]


    last_day_string = get_date_string(last_day)
    first_day_string = get_date_string(first_day)

    print(last_day_string)
    print(first_day_string)
    latest_trailing_12_months = daily_expenses.loc[first_day_string:last_day_string]
    print()
    print("30 largest expenses in the last 12 months:")
    # Used to show largest expenses over the last 12 months
    print(latest_trailing_12_months.sort_values("Money Out").head(30)["Money Out"].sum())

    print("largest expenses in the last 12 months sorted by name:")
    print(latest_trailing_12_months.sort_values("Money Out").head(30))
    # Need to sort this somehow
    print(latest_trailing_12_months.sort_values("Money Out").groupby("Name")["Money Out"].sum().nsmallest(20))

    # ---12 month rolling window calculations
    print()
    print("12 month Trailing expenses TTM means Trailing Twelve Months:")
    daily_expenses_by_month = daily_expenses[["Money Out"]].groupby(pd.Grouper(freq='ME')).sum()

    df_12_month_trailing_expenses = daily_expenses_by_month.rolling("365d",
                                                                             on=daily_expenses_by_month.index).sum()
    df_12_month_trailing_expenses["TTM / 12"] = df_12_month_trailing_expenses["Money Out"] / 12
    df_12_month_trailing_expenses["Actual Month Expense"] = daily_expenses_by_month["Money Out"]
    df_12_month_trailing_expenses.rename(columns={"Money Out": "TTM"})
    print(df_12_month_trailing_expenses)

    # Get the current month and year
    current_month = datetime.now().month
    current_year = datetime.now().year

    # Calculate the start and end of the tax year
    if current_month >= 4:
        start_of_tax_year = datetime(current_year, 4, 1)
    else:
        start_of_tax_year = datetime(current_year - 1, 4, 1)

    end_of_tax_year = last_day

    # Filter the DataFrame for the latest tax year and rename it to df_tax_year
    df_tax_year = df_12_month_trailing_expenses[
        (df_12_month_trailing_expenses.index >= start_of_tax_year) & (
                    df_12_month_trailing_expenses.index <= end_of_tax_year)
        ]

    # Sum the "Actual Month Expense" column
    total_actual_month_expense = df_tax_year["Actual Month Expense"].sum()

    # Print the total actual month expense for the latest tax year
    print("Total Actual Previous Month Expenses for the latest tax year:", total_actual_month_expense)

    print(df_tax_year)


    # Calculate the number of months in the tax year
    months_in_tax_year = (end_of_tax_year.year - start_of_tax_year.year) * 12 + (end_of_tax_year.month - start_of_tax_year.month) + 1
    num_rows = df_tax_year["Actual Month Expense"].size
    print(f"Number of previous months the tax year: {num_rows}")
    surplus = 45000/12 * num_rows + total_actual_month_expense
    print(f"Money left over from previous months in the tax year vs budget: {surplus}")



    # # Filter the DataFrame for the latest tax year and rename it to df_tax_year
    # df_tax_year = df_12_month_trailing_expenses[df_12_month_trailing_expenses["Date"].dt.month >= 4]
    # df_tax_year = df_tax_year[df_tax_year["Date"] <= last_day]
    #
    # # Sum the "Actual Month Expense" column
    # total_actual_month_expense = df_tax_year["Actual Month Expense"].sum()
    #
    # # Print the total actual month expense for the latest tax year
    # print("Total Actual Month Expense for the latest tax year:", total_actual_month_expense)



    # TODO ---- Maybe this should be its own seperate function?
    # and probably last month should be inferred, or a parameter?

    beginning_of_last_month_date = last_day.replace(day=1)
    beginning_of_last_month_date_string = get_date_string(beginning_of_last_month_date)
    df_last_month = daily_expenses.loc[beginning_of_last_month_date_string:last_day_string]
    # More detail on top 20 expenses for last month
    print()
    print("Total last month:")
    print(df_last_month["Money Out"].sum())
    print("20 largest transactions in the last month:")
    print(df_last_month.sort_values("Money Out").head(20))
    print(df_last_month.sort_values("Money Out").head(20)["Money Out"].sum())


    print("20 largest expenses in the last month sorted by name:")
    print(last_day_string)
    # Need to sort this somehow
    print(df_last_month.sort_values("Money Out").groupby("Name")["Money Out"].sum().nsmallest(20))
    print(df_last_month.sort_values("Money Out").groupby("Name")["Money Out"].sum().nsmallest(20)[:-1].sum())

def print_expense_stats(transactions, excluded_names):
    print_expense_stats_trailing_twelve_months(transactions, excluded_names, datetime.today())