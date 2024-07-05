from collections import defaultdict
import csv
from datetime import datetime
import glob
import os

import pandas as pd

from .manager import StorageManager

from ..process import convert_transactions_to_usable_data

# TODO: Store this in a better place

class FileManager(StorageManager):

    master_filename = 'data\\all_transactions.csv'
    master_columns = [
        'date',
        'description',
        'amount',
        'institution',
        'category',
        'id',
        'hash',
        'human_confirmed'
    ]
    filetype_regexes = list([
        'amex*',
        'chase*',
        'navyfed*',
        'becu*'
    ])

    def __init__(self) -> None:
        self.transactions = None
        self.updated = False

    # default dates can be any window that we won't need transactions outside of
    def get_transactions(self, start_date: str = '01/01/0001', end_date: str = '01/01/2100') -> pd.DataFrame:
        if self.transactions is None:
            self.transactions = pd.read_csv(
                FileManager.master_filename,
                names=FileManager.master_columns
            )
            self.transactions['date'] = pd.to_datetime(self.transactions['date'])

        start = datetime.strptime(start_date, '%m/%d/%Y')
        end = datetime.strptime(end_date, '%m/%d/%Y')

        dated_transactions = self.transactions.loc[
            (self.transactions['date'] >= start) &
            (self.transactions['date'] <= end)
        ]

        return dated_transactions

    def update_transactions(self, updated_transactions: pd.DataFrame) -> None:
        transactions = self.get_transactions()

        for _, date, description, amount, institution, category, id, _, human_confirmed in updated_transactions.itertuples():
            transactions.loc[transactions['id'] == id, 'date'] = date
            transactions.loc[transactions['id'] == id, 'description'] = description
            transactions.loc[transactions['id'] == id, 'amount'] = amount
            transactions.loc[transactions['id'] == id, 'institution'] = institution
            transactions.loc[transactions['id'] == id, 'category'] = category
            transactions.loc[transactions['id'] == id, 'human_confirmed'] = human_confirmed

        self.transactions = transactions
        self.updated = True

    def save(self) -> None:
        if not self.updated: return

        self.transactions.to_csv(FileManager.master_filename, header=False, index=False)
        self.updated = False

    def load_new_transactions(self) -> None:
        filetype_to_transactions = defaultdict(list)
        for filetype_regex in FileManager.filetype_regexes:
            for filename in glob.glob(filetype_regex):
                filetype = FileManager._convert_filename_to_filetype(filename)
                with open(filename, 'r') as csvfile:
                    reader = csv.reader(csvfile)
                    for i, row in enumerate(reader):
                        # these files have a header by default
                        if not i: continue
                        filetype_to_transactions[filetype].append(row)

                os.remove(filename)


        processed_transactions = convert_transactions_to_usable_data(filetype_to_transactions)
        transactions = self.get_transactions()
        all_hashes = set(transactions['hash'])

        new_transactions_to_add = list()

        for transaction in processed_transactions:
            if transaction.hash in all_hashes: continue
            new_transactions_to_add.append(list())

            new_transactions_to_add[-1].append(transaction.date)
            new_transactions_to_add[-1].append(transaction.description)
            new_transactions_to_add[-1].append(float(transaction.amount))
            new_transactions_to_add[-1].append(transaction.institution)
            new_transactions_to_add[-1].append(transaction.category)
            new_transactions_to_add[-1].append(transaction.guid)
            new_transactions_to_add[-1].append(transaction.hash)
            new_transactions_to_add[-1].append(int(transaction.human_confirmed))

        self.transactions = pd.concat((
            pd.DataFrame(new_transactions_to_add, columns=FileManager.master_columns),
            transactions
        ))

        self.updated = True


    def _convert_filename_to_filetype(filename: str) -> str:
        if 'amex' in filename:
            return 'amex'
        elif 'chase' in filename:
            return 'chase'
        elif 'navyfed' in filename:
            return 'navyfed'
        elif 'becu' in filename:
            return 'becu'
        else:
            raise NotImplementedError(f'{filename} not implemented')