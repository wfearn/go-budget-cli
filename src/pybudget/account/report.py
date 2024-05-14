from collections import defaultdict
from datetime import datetime

import pandas as pd

def get_spending(transactions: pd.DataFrame):

    category_to_spending = defaultdict(float)
    dates = list()

    for transaction in transactions.itertuples():
        amounts = transaction.amount.split(',')
        categories = transaction.category.split(',')
        dates.append(transaction.date)

        if categories == 'TO_LABEL': continue

        for i in range(len(amounts)):
            category_to_spending[categories[i]] += float(amounts[i])

    dates.sort()
    start_date = dates[0]
    end_date = dates[-1]
    total_amount = float(0)
    print(f'Spending from {start_date} to {end_date}')
    for category, spending_amount in category_to_spending.items():
        total_amount += spending_amount
        if spending_amount < 0:
            spending_amount *= -1
        print(f'\t{category}: {spending_amount}')
    print(f'Total: {total_amount}')