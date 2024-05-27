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
from sklearn.neural_network import MLPRegressor, MLPClassifier
from sklearn.svm import LinearSVC


PreparedTransaction = namedtuple('PreparedTransaction', 'transaction_string categories amounts total_amount')


class LabellingAssistant:
    model_name_to_class = {
        'svm': LinearSVC,
        'mlpr': MLPRegressor,
        'mlpc': MLPClassifier
    }

    vectorizer_name_to_class = {
        'tfidf': TfidfVectorizer
    }

    def __init__(self, category_model='mlpc', amount_model='mlpr', vectorizer = 'tfidf'):
        self.category_model_trained = False
        self.amount_model_trained = False
        self.vectorizer_trained = False

        # TODO: Interface with StorageManager outside of this class for these models
        if os.path.exists(category_model):
            with open(category_model, 'rb') as f:
                self.category_model, self.category_to_label = pickle.load(f)
                self.category_model_trained = True

        else:
            self.category_model = LabellingAssistant.model_name_to_class[category_model]()
            self.category_to_label = None

        if os.path.exists(amount_model):
            with open(amount_model, 'rb') as f:
                self.amount_model = pickle.load(f)
                self.amount_model_trained = True
        else:
            self.amount_model = LabellingAssistant.model_name_to_class[amount_model](hidden_layer_sizes=(100, 50))

        if os.path.exists(vectorizer):
            with open(vectorizer, 'rb') as f:
                self.vectorizer = pickle.load(f)
                self.vectorizer_trained = True

        else:
            self.vectorizer = LabellingAssistant.vectorizer_name_to_class[vectorizer](analyzer='char', ngram_range=(1, 2))

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

        training_transactions = list()

        for transaction in transactions.itertuples():
            prepared_transaction = self.prepare_transaction_for_featurization(transaction)
            for training_transaction, category_label, amount_label in self.expand_prepared_transactions_into_training_data(prepared_transaction):
                training_transactions.append((training_transaction, category_label, amount_label))

        train_and_validation, test = train_test_split(training_transactions, test_size=0.2)
        train, validation = train_test_split(train_and_validation, test_size=0.2)

        test_data, test_category_labels, test_amount_labels = zip(*test)
        train_data, train_category_labels, train_amount_labels = zip(*train)
        validation_data, validation_category_labels, validation_amount_labels = zip(*validation)

        self.category_to_label = { category: i for i, category in enumerate(set(train_category_labels)) }

        for l in validation_category_labels:
            self.category_to_label.setdefault(l, len(self.category_to_label))

        for l in test_category_labels:
            self.category_to_label.setdefault(l, len(self.category_to_label))

        train_Y = np.array([self.category_to_label[category] for category in train_category_labels])
        test_Y = np.array([self.category_to_label[category] for category in test_category_labels])
        validation_Y = np.array([self.category_to_label[category] for category in validation_category_labels])

        self.train_vectorizer(train_data)

        train_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in train_data ]))
        test_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in test_data ]))
        validation_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in validation_data ]))

        self.category_model.fit(train_X, train_Y)

        print(f'Category Model Score: {self.category_model.score(validation_X, validation_Y)}')
    
    def train_amount_model(self, transactions: pd.DataFrame, retrain: bool = False) -> None:
        if self.amount_model_trained and not retrain:
            return

        training_transactions = list()

        for transaction in transactions.itertuples():
            prepared_transaction = self.prepare_transaction_for_featurization(transaction)
            for training_transaction, category_label, amount_label in self.expand_prepared_transactions_into_training_data(prepared_transaction):
                training_transactions.append((training_transaction, category_label, amount_label))

        train_and_validation, test = train_test_split(training_transactions, test_size=0.2)
        train, validation = train_test_split(train_and_validation, test_size=0.2)

        test_data, test_category_labels, test_amount_labels = zip(*test)
        train_data, train_category_labels, train_amount_labels = zip(*train)
        validation_data, validation_category_labels, validation_amount_labels = zip(*validation)

        for i in range(len(test_data)):
            test_data[i].categories.append(test_category_labels[i])

        for i in range(len(train_data)):
            train_data[i].categories.append(train_category_labels[i])

        for i in range(len(validation_data)):
            validation_data[i].categories.append(validation_category_labels[i])

        train_Y = np.array(train_amount_labels)
        test_Y = np.array(test_amount_labels)
        validation_Y = np.array(validation_amount_labels)

        train_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in train_data ]))
        test_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in test_data ]))
        validation_X = vstack(np.array([ self.featurize_prepared_transaction(d) for d in validation_data ]))

        self.amount_model.fit(train_X, train_Y)

        print(f'Amount Model Score: {self.amount_model.score(validation_X, validation_Y)}')
    
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

        # NOTE: We only expect there to be a maximum number of 5 categories per transaction -- could change
        amount_vector = np.zeros(5)
        for i, amount in enumerate(prepared_transaction.amounts):
            amount_vector[i] = amount

        amounts = np.expand_dims(amount_vector, 0)
        total_amount = np.expand_dims(np.array([prepared_transaction.total_amount]), 0)

        X = hstack((X_vector, amounts, total_amount))

        return X

    def prepare_transaction_for_featurization(self, transaction: Tuple) -> Tuple[str, List[str], List[float], float]:

        amounts = transaction.amount.split(',')
        categories = [ c for c in transaction.category.split(',') if c != 'TO_LABEL' ]

        date, description, institution = transaction.date, transaction.description, transaction.institution
        initial_string = f'{date} {description} {institution}'

        float_amounts = [ float(a) for a in amounts ]
        total_amount = sum(float_amounts)

        return PreparedTransaction(initial_string, categories, float_amounts, total_amount)

    def label_transactions(self, transactions: pd.DataFrame, labels: List[str]) -> pd.DataFrame:

        transactions = transactions.copy()

        label_to_category = { label: category for category, label in self.category_to_label.items() }

        while 'TO_LABEL' in transactions['category'].unique():

            num_samples = min(20, len(transactions))
            sampled_transactions = transactions.sample(n=num_samples)

            for transaction in sampled_transactions.itertuples():

                prepared_transaction = self.prepare_transaction_for_featurization(transaction)
                prepared_transaction.amounts.clear()

                while True:

                    featurized_transaction = self.featurize_prepared_transaction(prepared_transaction)

                    model_prediction = self.category_model.predict(featurized_transaction)
                    predicted_index = model_prediction[0]
                    predicted_category = label_to_category[predicted_index]
                    confirmed_category = self.confirm_predicted_category(prepared_transaction, predicted_category, labels)


                    if confirmed_category == 'NONE':
                        break

                    prepared_transaction.categories.append(confirmed_category)


                    featurized_transaction = self.featurize_prepared_transaction(prepared_transaction)
                    predicted_amount = self.amount_model.predict(featurized_transaction)
                    confirmed_amount = self.confirm_predicted_amount(predicted_amount)
                    prepared_transaction.amounts.append(confirmed_amount)
                    print('\n\n')



        return transactions

    def confirm_predicted_category(
        self,
        transaction: Tuple[str, List[str], List[float], float],
        predicted_category: str,
        labels: List[str]
    ) -> str:

        string, categories, amounts, total_amount = transaction.transaction_string, transaction.categories, transaction.amounts, transaction.total_amount
        confirmed_category = predicted_category

        print('Transaction:', string)
        print('Total Amount:', total_amount)
        print('Categories So Far:', categories)
        print('Amounts So Far:', amounts)
        confirmation = input(f'Does {predicted_category} match this transaction? ')

        if confirmation != 'y':
            num_to_label = { i: label for i, label in enumerate(labels) }
            num_to_label[len(num_to_label)] = 'NONE'
            for i, label in num_to_label.items():
                print(i, ':', label)

            num = int(input('\tWhich label matches this transaction? '))
            confirmed_category = num_to_label[num]

        return confirmed_category

    def confirm_predicted_amount(self, predicted_amount: float) -> float:

        confirmed_amount = predicted_amount[0]

        confirmation = input(f'Does {confirmed_amount} seem about right for this category? ')
        print()

        if confirmation != 'y':
            confirmed_amount = float(input('\tWhat amount matches this category? '))

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

