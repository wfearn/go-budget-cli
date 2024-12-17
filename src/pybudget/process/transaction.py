from collections import namedtuple
import hashlib
from typing import Dict, List, Tuple, Any
import uuid

from .extract import TransactionExtractorPipeline


TransactionWithHash = namedtuple('TransactionWithHash', 'date description amount extractor_id category transaction_indicator guid hash human_confirmed')


def generate_additional_transaction_data(transaction: tuple) -> namedtuple:

    date, description, amount, transaction_indicator, extractor_id = transaction
    hash_string = f'{date.strftime("%m/%d/%Y")}{description}{amount}{extractor_id}{transaction_indicator}'
    hash_digest = hashlib.sha256(str.encode(hash_string)).hexdigest()
    guid = str(uuid.uuid4())
    category = 'TO_LABEL'
    human_confirmed = 0

    return TransactionWithHash(
        date,
        description,
        amount,
        extractor_id, 
        category,
        transaction_indicator,
        guid,
        hash_digest,
        human_confirmed
    )


def convert_transactions_to_usable_data(transactions: List[Tuple[Any]]) -> bool:
    transaction_extractor_pipeline = TransactionExtractorPipeline()
    new_processed_transactions = transaction_extractor_pipeline.extract_transactions(transactions)

    new_processed_transactions = map(generate_additional_transaction_data, new_processed_transactions)

    return new_processed_transactions