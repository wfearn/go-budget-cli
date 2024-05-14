from collections import namedtuple
import hashlib
from typing import Dict, List, Tuple, Any
import uuid

from ..parse.parse import (
    parse_transaction
)


TransactionWithHash = namedtuple('TransactionWithHash', 'date description amount institution category guid hash human_confirmed')


def generate_additional_transaction_data(transaction: tuple) -> namedtuple:

    date, description, amount, institution = transaction
    hash_string = f'{date.strftime("%m/%d/%Y")}{description}{amount}{institution}'
    hash_digest = hashlib.sha256(str.encode(hash_string)).hexdigest()
    guid = str(uuid.uuid4())
    category = 'TO_LABEL'
    human_confirmed = 0

    return TransactionWithHash(date, description, amount, institution, category, guid, hash_digest, human_confirmed)


def convert_transactions_to_usable_data(transactions: Dict[str, List[Tuple[Any]]]) -> bool:
    new_processed_transactions = [
        parse_transaction(transaction, transaction_type)
        for transaction_type, raw_transaction_list in transactions.items()
        for transaction in raw_transaction_list
    ]

    new_processed_transactions = map(generate_additional_transaction_data, new_processed_transactions)

    return new_processed_transactions