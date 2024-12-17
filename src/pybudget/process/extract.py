from abc import ABC, abstractmethod
from collections import namedtuple
from typing import List


ExtractedTransaction = namedtuple('ExtractedTransaction', 'date description amount credit_or_debit')

class InvalidSchemaError(Exception):
    pass

class InvalidTransactionIndicatorError(Exception):
    pass


class ChainOfResponsibilityTransactionExtractor(ABC):
    @abstractmethod
    def extract_transactions(self, transactions: List[List[str]]) -> List[ExtractedTransaction]:
        raise NotImplementedError

    
class TransactionExtractorTemplate(ABC):
    credit_indicator_string = 'Credit'
    debit_indicator_string = 'Debit'

    @classmethod
    def extract(cls, transaction: List[str]) -> ExtractedTransaction:
        date, amount, credit_debit_indicator, description = cls._extraction_method(transaction)

        date = cls.normalize_date(date)
        credit_or_debit = cls.normalize_credit_debit_indicator(credit_debit_indicator)
        amount = cls.normalize_amount(amount, credit_or_debit)

        return ExtractedTransaction(date, description, amount, credit_or_debit)

    @abstractmethod
    def _extraction_method(self, transaction: List[str]) -> ExtractedTransaction:
        raise NotImplementedError

    @classmethod
    def normalize_date(self, date_string: str) -> str:
        return date_string
    
    @classmethod
    def normalize_amount(self, amount_string: str, credit_or_debit: str) -> str:
        if credit_or_debit == TransactionExtractorTemplate.credit_indicator_string:
            amount_string = str(-1 * abs(float(amount_string)))
        elif credit_or_debit == TransactionExtractorTemplate.debit_indicator_string:
            amount_string = str(abs(float(amount_string)))
        else:
            raise InvalidTransactionIndicatorError(
                f'{credit_or_debit} invalid way to indicate transaction status'
            )

        return amount_string

    @classmethod
    def normalize_credit_debit_indicator(cls, indicator_string: str) -> str:
        return indicator_string

    
class SchemaOneExtractor(TransactionExtractorTemplate):
    @classmethod
    def _extraction_method(cls, transaction: List[str]) -> ExtractedTransaction:
        date, amount, credit_or_debit, _, _, _, _, _, _, description, _, _, _ = transaction
        return date, amount, credit_or_debit, description

    
class SchemaTwoExtractor(TransactionExtractorTemplate):
    @classmethod
    def _extraction_method(cls, transaction: List[str]) -> ExtractedTransaction:
        _, date, amount, credit_or_debit, _, _, _, _, _, _, description, _, _ = transaction
        return date, amount, credit_or_debit, description


class SchemaThreeExtractor(TransactionExtractorTemplate):
    @classmethod
    def _extraction_method(cls, transaction: List[str]) -> ExtractedTransaction:
        date, _, description, _, _, amount, _ = transaction
        credit_or_debit = (
            TransactionExtractorTemplate.debit_indicator_string
            if float(amount) < 0 else
            TransactionExtractorTemplate.credit_indicator_string
        )
        return date, amount, credit_or_debit, description


class SchemaFourExtractor(TransactionExtractorTemplate):
    @classmethod
    def _extraction_method(cls, transaction: List[str]) -> ExtractedTransaction:
        date, description, amount = transaction
        credit_or_debit = (
            TransactionExtractorTemplate.credit_indicator_string
            if float(amount) < 0 else
            TransactionExtractorTemplate.debit_indicator_string
        )
        return date, amount, credit_or_debit, description

   
class TransactionExtractorPipeline(ChainOfResponsibilityTransactionExtractor):
    def __init__(self):
        self.extractors = list()
        self.extractors.append(SchemaOneExtractor.extract)
        self.extractors.append(SchemaTwoExtractor.extract)
        self.extractors.append(SchemaThreeExtractor.extract)
        self.extractors.append(SchemaFourExtractor.extract)

    def extract_transactions(self, transactions: List[List[str]]) -> List[ExtractedTransaction]:
        extracted_transactions = list()

        for transaction in transactions:
            extraction_successful = False
            for extraction_method in self.extractors:
                try:
                    extracted_transaction = extraction_method(transaction)
                except (InvalidTransactionIndicatorError, ValueError):
                    continue

                extraction_successful = True
                extracted_transactions.append(extracted_transaction)
                

            if not extraction_successful:
                raise InvalidSchemaError(
                    'This schema is not recognized, please update the code and the extractor to properly parse this schema'
                )
        
        return extracted_transactions