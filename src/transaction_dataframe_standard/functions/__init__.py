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
    print(latest_trailing_12_months.sort_values("Money Out").head(30))
    print(latest_trailing_12_months.sort_values("Money Out").head(30)["Money Out"].sum())

    # ---12 month rolling window calculations
    print()
    print("12 month Trailing expenses:")
    daily_expenses_by_month = daily_expenses[["Money Out"]].groupby(pd.Grouper(freq='ME')).sum()

    df_12_month_trailing_expenses = daily_expenses_by_month.rolling("365d",
                                                                             on=daily_expenses_by_month.index).sum()
    df_12_month_trailing_expenses["Monthly Money Out"] = df_12_month_trailing_expenses["Money Out"] / 12
    print(df_12_month_trailing_expenses)

    # --- Expenses per month
    print()
    print("Expenditure broken down per month:")
    print(daily_expenses_by_month)

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

def print_expense_stats(transactions, excluded_names):
    print_expense_stats_trailing_twelve_months(transactions, excluded_names, datetime.today())