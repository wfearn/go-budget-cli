from collections import namedtuple
from datetime import datetime

import pandas as pd

NEW_COLUMN_NAMES = ['Date', 'Description', 'Category', 'Type', 'Amount']

Transaction = namedtuple('Transaction', 'date description amount institution')

def parse_transaction(transaction: str, transaction_type: str) -> namedtuple:
    ## TODO: change these to dictionaries and pull the columns I want
    if transaction_type == 'amex':
        date, description, amount = transaction
    elif transaction_type == 'chase':
        date, _, description, _, _, amount, _ = transaction
        if amount[0] == '-':
            amount = amount[1:]
        else:
            amount = f'-{amount}'

    elif transaction_type == 'navyfed':
        date, amount, credit_or_debit, _, _, _, _, _, _, description, _, _, _ = transaction

        if credit_or_debit == 'Credit':
            amount = f'-{amount}'
    
    elif transaction_type == 'becu':
        date, _, description, debit, credit = transaction
        amount = f'-{credit}' if credit else debit[1:]


    else:
        raise NotImplementedError(f'{transaction_type} not implemented')

    return Transaction(datetime.strptime(date, '%m/%d/%Y'), description, float(amount), transaction_type)