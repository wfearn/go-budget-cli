from collections import defaultdict
import csv
from datetime import datetime
import glob
import io
import os
from typing import Dict, List

import pandas as pd
import yaml

from .manager import StorageManager

from ..process import convert_transactions_to_usable_data


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
        self.transactions_updated = True

    def save(self) -> None:
        if self.budget_updated:
            with io.open(self.main_budget_filename, 'w') as f:
                yaml.dump(self.budget, f, default_flow_style=False)

            self.budget_updated = False

        if self.transactions_updated:
            self.transactions.to_csv(FileManager.master_filename, header=False, index=False)
            self.transactions_updated = False

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

        self.transactions_updated = True


    def _convert_filename_to_filetype(filename: str) -> str:
        if 'amex' in filename:
            return 'amex'
        elif 'chase' in filename:
            return 'chase'
        elif 'navyfed' in filename:
            return 'navyfed'
        elif 'becu' in filename:
            return 'becu'
        elif 'sofi' in filename:
            return 'sofi'
        else:
            raise NotImplementedError(f'{filename} not implemented')

class CSVFileManager(FileManager):
    budget_directory = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/budget'
    data_directory = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data'
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
        filename = f'{self.data_directory}/{filename}' if self._save_in_default_dir(
            filename
        ) else filename

        with open(filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for row in data:
                writer.writerow(row)

