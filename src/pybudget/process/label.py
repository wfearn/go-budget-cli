from collections import namedtuple
from itertools import permutations
import os
import pickle
from typing import List, Tuple, Generator

import numpy as np
import pandas as pd
from scipy.sparse import hstack, vstack
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.neural_network import MLPRegressor
from sklearn.svm import LinearSVC


PreparedTransaction = namedtuple('PreparedTransaction', 'transaction_string categories amounts total_amount')


class LabellingAssistant:
    model_name_to_class = {
        'svm': LinearSVC,
        'nn': MLPRegressor
    }

    vectorizer_name_to_class = {
        'tfidf': TfidfVectorizer
    }

    def __init__(self, category_model='svm', amount_model='nn', vectorizer = 'tfidf'):
        self.category_model_trained = False
        self.amount_model_trained = False
        self.vectorizer_trained = False

        # TODO: Interface with StorageManager outside of this class for these models
        if os.path.exists(category_model):
            with open(category_model, 'rb') as f:
                self.category_model = pickle.load(f)
                self.category_model_trained = True

        else:
            self.category_model = LabellingAssistant.model_name_to_class[category_model]()

        if os.path.exists(amount_model):
            with open(amount_model, 'rb') as f:
                self.amount_model = pickle.load(f)
                self.amount_model_trained = True
        else:
            self.amount_model = LabellingAssistant.model_name_to_class[amount_model]()

        if os.path.exists(vectorizer):
            with open(vectorizer, 'rb') as f:
                self.vectorizer = pickle.load(f)
                self.vectorizer_trained = True

        else:
            self.vectorizer = LabellingAssistant.vectorizer_name_to_class[vectorizer](analyzer='char', ngram_range=(1, 5))

    def expand_prepared_transactions_into_training_data(
        self,
        prepared_transaction: Tuple[str, List[str], List[float], float]
    ) -> Generator[
            Tuple[str, List[str], List[float], float],
            str,
            float
        ]:

        num_categories = len(prepared_transaction.categories)
        amounts_and_categories = zip(prepared_transaction.amounts, prepared_transaction.categories)

        for p in range(num_categories + 1):
            for permutation in permutations(amounts_and_categories, p):
                permuted_amounts, permuted_categories = zip(*permutation) if p else list(), list()

                category_labels = list()
                amount_labels = list()

                for i, category in enumerate(prepared_transaction.categories):
                    if category not in permuted_categories:
                        category_labels.append(category)
                        amount_labels.append(prepared_transaction.amounts[i])

                if p == num_categories:
                    category_labels.append('NONE')
                    amount_labels.append(0.0)

                for i, category_label in enumerate(category_labels):
                    amount_label = amount_labels[i]
                    yield (
                        PreparedTransaction(
                            prepared_transaction.transaction_string,
                            permuted_categories,
                            permuted_amounts,
                            prepared_transaction.total_amount
                        ),
                        category_label,
                        amount_label
                    )


    def train_category_model(self, transactions: pd.DataFrame, retrain: bool = False) -> None:
        if self.category_model_trained and not retrain:
            return

        print(len(transactions))

        training_transactions = list()

        for transaction in transactions.itertuples():
            prepared_transaction = self.prepare_transaction_for_featurization(transaction)
            for training_transaction, category_label, amount_label in self.expand_prepared_transactions_into_training_data(prepared_transaction):
                training_transactions.append((training_transaction, category_label, amount_label))

        print(len(training_transactions))

        train_and_validation, test = train_test_split(training_transactions, test_size=0.2)
        train, validation = train_test_split(train_and_validation, test_size=0.2)

        test_data, test_category_labels, test_amount_labels = zip(*test)
        train_data, train_category_labels, train_amount_labels = zip(*train)
        validation_data, validation_category_labels, validation_amount_labels = zip(*validation)

        category_to_label = { category: i for i, category in enumerate(train_category_labels) }

        for l in validation_category_labels:
            category_to_label.setdefault(l, len(category_to_label))

        for l in test_category_labels:
            category_to_label.setdefault(l, len(category_to_label))
                

        train_Y = np.array([category_to_label[category] for category in train_category_labels])
        test_Y = np.array([category_to_label[category] for category in test_category_labels])
        validation_Y = np.array([category_to_label[category] for category in validation_category_labels])

        self.train_vectorizer(train_data)

        train_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in train_data ]))
        test_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in test_data ]))
        validation_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in validation_data ]))

        self.category_model.fit(train_X, train_Y)

        print(self.category_model.score(validation_X, validation_Y))
    
    def train_amount_model(self, transaction: pd.DataFrame) -> None:
        raise NotImplementedError
    
    def train_vectorizer(self, training_data: List[Tuple[str, List[str], List[float], float]], retrain: bool = False) -> None:
        if self.vectorizer_trained and not retrain:
            return

        string_data = list()

        for datum in training_data:
            category_string = ' '.join(datum.categories)
            string_datum = f'{datum.transaction_string} {category_string}'
            string_data.append(string_datum)

        self.vectorizer.fit(string_data)

    def featurize_prepared_transaction(self, prepared_transaction: Tuple[str, List[str], List[float], float]) -> np.array:

        category_string = ' '.join(prepared_transaction.categories)
        X_vector = self.vectorizer.transform([f'{prepared_transaction.transaction_string} {category_string}'])

        # We only expect there to be a maximum number of 5 categories per transaction -- could change
        amount_vector = np.zeros(5)
        for i, amount in enumerate(prepared_transaction.amounts):
            amount_vector[i] = amount

        amounts = np.expand_dims(amount_vector, 0)
        total_amount = np.expand_dims(np.array([prepared_transaction.total_amount]), 0)

        X = hstack((X_vector, amounts, total_amount))

        return X

    def prepare_transaction_for_featurization(self, transaction: Tuple) -> Tuple[str, List[str], List[float], float]:

        amounts = transaction.amount.split(',')
        categories = transaction.category.split(',')

        date, description, institution = transaction.date, transaction.description, transaction.institution
        initial_string = f'{date} {description} {institution}'

        total_amount = sum([float(a) for a in amounts])

        return PreparedTransaction(initial_string, categories, amounts, total_amount)

    def label_transactions(self, transactions: pd.DataFrame, labels: List[str]) -> pd.DataFrame:

        transactions = transactions.copy()

        label_to_category = { i: label for i, label in enumerate(labels) }

        while 'TO_LABEL' in transactions['category'].unique():

            num_samples = min(20, len(transactions))
            sampled_transactions = transactions.sample(n=num_samples)

            for transaction in sampled_transactions.itertuples():

                prepared_transaction = self.prepare_transaction_for_featurization(transaction)

                while True:

                    featurized_transaction = self.featurize_prepared_transaction(prepared_transaction)

                    # predicted_category = label_to_category[self.category_model.predict(featurized_transaction)]
                    predicted_category = label_to_category[0]
                    confirmed_category = self.confirm_predicted_category(transaction, predicted_category, labels)
                    prepared_transaction.categories.append(confirmed_category)

                    featurized_transaction = self.featurize_prepared_transaction(prepared_transaction)
                    # predicted_amount = self.amount_model.predict(featurized_transaction)
                    predicted_amount = 10.0
                    confirmed_amount = self.confirm_predicted_amount(transaction, confirmed_category, predicted_amount)
                    prepared_transaction.amounts.append(confirmed_amount)

                    print('Prepared Transaction:', prepared_transaction)

                    if confirmed_category == 'NONE':
                        break


        return transactions

    def confirm_predicted_category(
        self,
        transaction: pd.Series,
        predicted_category: str,
        labels: List[str]
    ) -> str:

        date, description, amount = transaction.date, transaction.description, transaction.amount
        confirmed_category = predicted_category

        print('Date:', date)
        print('Description:', description)
        print('Amount:', amount)
        confirmation = input(f'Does {predicted_category} match this transaction? ')
        print()

        if confirmation != 'y':
            num_to_label = { i: label for i, label in enumerate(labels) }
            num_to_label[len(num_to_label)] = 'NONE'
            for i, label in num_to_label.items():
                print(i, ':', label)

            num = int(input('Which label matches this transaction? '))
            confirmed_category = num_to_label[num]

        return confirmed_category

    def confirm_predicted_amount(
        self,
        transaction: pd.Series,
        confirmed_category: str,
        predicted_amount: float
    ) -> float:

        confirmed_amount = predicted_amount

        date, description, amount = transaction.date, transaction.description, transaction.amount

        print('Date:', date)
        print('Description:', description)
        print('Amount:', amount)
        print('Predicted Category:', confirmed_category)
        confirmation = input(f'Does {predicted_amount} match the predicted category for this total amount? ')
        print()

        if confirmation != 'y':
            confirmed_amount = float(input('What amount matches this category? '))

        return confirmed_amount


def get_human_labels(transactions: pd.DataFrame, possible_labels: dict[int, str]) -> List[List[str]]:

    labels = list()
    amounts = list()

    for transaction in transactions.itertuples():
        transaction_labels = list()
        transaction_amounts = list()

        print('Transaction:')
        print('\tDate:', transaction.date)
        print('\tDescription:', transaction.description)
        print('\tAmount:', transaction.amount)
        print('\tInstitution:', transaction.institution)
        print('\tPossible Labels:', end='')
        for key, label in possible_labels.items():
            print(f' {key}:{label}', end='')
        print()
        label = input('What are the labels for this transaction? ')
        if label.isnumeric():
            transaction_labels.append(possible_labels[int(label)])
        else:
            transaction_labels.append(label)

        total_amount = float(transaction.amount)
        transaction_amounts.append(total_amount)

        while True:
            label = input('\tDoes any other label apply? ')
            if not label:
                break
            sub_amount = float(input(f'\t\tHow much of the amount does this account for (Total Amount: {total_amount})? '))
            transaction_amounts.append(sub_amount)
            transaction_amounts[0] -= sub_amount

            if label.isnumeric():
                transaction_labels.append(possible_labels[int(label)])
            else:
                transaction_labels.append(label)

        labels.append(','.join(transaction_labels))
        amounts.append(','.join([str(a) for a in transaction_amounts]))

        assert sum(transaction_amounts) == total_amount
        
    return amounts, labels

