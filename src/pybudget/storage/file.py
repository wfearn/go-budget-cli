import csv
from datetime import datetime
import glob
import io
import os
from pathlib import Path
from typing import Dict, List

import pandas as pd
import yaml

from pybudget.process import convert_transactions_to_usable_data

from .manager import StorageManager


class FileManager(StorageManager):

    def __init__(self) -> None:
        if not os.path.isdir(self.data_directory):
            os.makedirs(self.data_directory)

        if not os.path.isdir(self.budget_directory):
            os.makedirs(self.budget_directory)

        self.budget = None
        self.budget_updated = False

        self.transactions = None
        self.transactions_updated = False

    def update_budget(self, budget: Dict[str, int]) -> None:
        self.budget = budget
        self.budget_updated = True

    def get_budget(self) -> Dict[str, int]:
        if self.budget is None:
            with open(self.main_budget_filename, 'r') as f:
                budget = yaml.safe_load(f)

            self.budget = budget

        return self.budget

    # default dates can be any window that we won't need transactions outside of
    def get_transactions(self, start_date: str = '01/01/0001', end_date: str = '01/01/2100') -> pd.DataFrame:
        if self.transactions is None:
            self.transactions = self.read(CSVFileManager.master_filename)

        start = datetime.strptime(start_date, '%m/%d/%Y')
        end = datetime.strptime(end_date, '%m/%d/%Y')

        dated_transactions = {}
        for i, transaction in enumerate(self.transactions):
            if i == 0: continue
            transaction_date = datetime.strptime(transaction[0], '%Y/%m/%d')
            if transaction_date > end or transaction_date < start: continue
            transaction_hash = transaction[-2]
            if transaction_hash in dated_transactions: continue
            dated_transactions[transaction_hash] = transaction

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
        self.transactions_updated = True

    def save(self) -> None:
        if self.budget_updated:
            with io.open(self.main_budget_filename, 'w') as f:
                yaml.dump(self.budget, f, default_flow_style=False)

            self.budget_updated = False

        if self.transactions_updated:
            self.write(self.transactions, CSVFileManager.master_filename)
            self.transactions_updated = False

    def load_new_transactions(self) -> None:
        new_transaction_pattern = os.path.join(
            self.data_directory,
            'new_transactions',
            '*.csv'
        )

        new_transactions = []
        for filename in glob.glob(new_transaction_pattern):
            with open(filename, 'r') as csvfile:
                reader = csv.reader(csvfile)
                for i, row in enumerate(reader):
                    # these files have a header by default
                    if not i: continue
                    new_transactions.append(row)

                # os.remove(filename)


        processed_transactions = convert_transactions_to_usable_data(new_transactions)
        previous_transactions = self.get_transactions()

        new_transactions_to_add = []

        for transaction in processed_transactions:
            if transaction.hash in previous_transactions: continue
            new_transactions_to_add.append([])

            new_transactions_to_add[-1].append(transaction.date)
            new_transactions_to_add[-1].append(transaction.description)
            new_transactions_to_add[-1].append(float(transaction.amount))
            new_transactions_to_add[-1].append(transaction.extractor_id)
            new_transactions_to_add[-1].append(transaction.category)
            new_transactions_to_add[-1].append(transaction.transaction_indicator)
            new_transactions_to_add[-1].append(transaction.guid)
            new_transactions_to_add[-1].append(transaction.hash)
            new_transactions_to_add[-1].append(int(transaction.human_confirmed))
        
        self.transactions.extend(new_transactions_to_add)

        self.transactions_updated = True


class CSVFileManager(FileManager):
    budget_directory = os.path.join(f'{Path.home()}', '.pybudget', 'budget')
    data_directory = os.path.join(f'{Path.home()}', '.pybudget', 'data')
    master_filename = 'all_transactions.csv'
    master_columns = [
        'date',
        'description',
        'amount',
        'institution',
        'category',
        'transaction_indicator',
        'id',
        'hash',
        'human_confirmed'
    ]

    def _save_in_default_dir(self, filename):
        return len(filename.split('/')) == 1

    def read(self, filename: str) -> List[List[str]]:
        filename = f'{self.data_directory}/{filename}' if self._save_in_default_dir(
            filename
        ) else filename

        data = list()

        with open(filename, newline='') as csvfile:
            reader = csv.reader(csvfile)
            for row in reader:
                data.append(row)

        return data

    def write(self, data: List[List[str]], filename: str) -> None:
        print('Filename:', filename)
        filename = os.path.join(f'{self.data_directory}', f'{filename}') if self._save_in_default_dir(
            filename
        ) else filename

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow(self.master_columns)
            for row in data:
                writer.writerow(row)

    def append(self, data: List[List[str]], filename: str) -> None:
        filename = f'{self.data_directory}/{filename}' if self._save_in_default_dir(
            filename
        ) else filename

        if not os.path.exists(filename):
            raise ValueError(f'{filename} must exist to append to it')
        
        with open(filename, 'a', newline='') as csvfile:
            writer = csv.writer(
                csvfile,
                delimiter=',',
                quotechar='"',
                quoting=csv.QUOTE_MINIMAL
            )
            for row in data:
                writer.writerow(row)