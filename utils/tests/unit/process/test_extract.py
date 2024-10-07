from typing import List
import pytest

from pybudget.process.extract import (
    ChainOfResponsibilityTransactionExtractor,
    TransactionExtractorTemplate,
    SchemaOneExtractor,
    SchemaTwoExtractor,
    SchemaThreeExtractor,
    SchemaFourExtractor
)

class MockTransactionExtractorTemplate(TransactionExtractorTemplate):
    def _extraction_method(self, transaction: List[str]):
        pass

class TestChainOfResponsibilityTransactionExtractor:
    def test_unable_to_instantiate(self):
        with pytest.raises(TypeError):
            chain = ChainOfResponsibilityTransactionExtractor()
    
    def test_unable_to_run_extract_transactions_method(self):
        test_data = [
            ['Test', 'Test', 'Test']
        ]
        with pytest.raises(TypeError):
            chain = ChainOfResponsibilityTransactionExtractor()
            chain.extract_transactions(test_data)

class TestTransactionExtractorTemplate:
    def test_unable_to_instantiate(self):
        with pytest.raises(TypeError):
            t = TransactionExtractorTemplate()

    def test_normalize_date_returns_date_passed_in(self):
        m = MockTransactionExtractorTemplate()
        test_date_string = '06/17/2024'
        assert m.normalize_date(test_date_string) == test_date_string

    def test_normalize_amount_returns_negative_value_if_credit(self):
        m = MockTransactionExtractorTemplate()
        test_amount = '300'
        test_indicator = 'Credit'
        assert float(m.normalize_amount(test_amount, test_indicator)) < 0

        test_amount = '-300'
        assert float(m.normalize_amount(test_amount, test_indicator)) < 0

    def test_normalize_amount_returns_positive_value_if_debit(self):
        m = MockTransactionExtractorTemplate()
        test_amount = '-300'
        test_indicator = 'Debit'
        assert float(m.normalize_amount(test_amount, test_indicator)) >= 0

        test_amount = '300'
        assert float(m.normalize_amount(test_amount, test_indicator)) >= 0

    def test_normalize_amount_does_not_accept_invalid_indicator(self):
        m = MockTransactionExtractorTemplate()
        test_amount = '300'
        test_indicator = 'Pizza'

        with pytest.raises(NotImplementedError):
            m.normalize_amount(test_amount, test_indicator)

    def test_normalize_credit_debit_indicator_is_identity_function(self):
        m = MockTransactionExtractorTemplate()
        test_indicator = 'Credit'
        assert m.normalize_credit_debit_indicator(test_indicator) == test_indicator

        test_indicator = 'Debit'
        assert m.normalize_credit_debit_indicator(test_indicator) == test_indicator

class TestSchemaOneExtractor:
    def test_extraction_method_succeeds_with_no_errors(self):
        extractor = SchemaOneExtractor()
        test_data = [
            '08/09/2024',
            '666.00',
            'Debit',
            'ACH Debit',
            'ACH Debit',
            '',
            '',
            '',
            '',
            'Description',
            'Another Description',
            '9999999',
            ''
        ]

        extractor.extract(test_data)

    def test_extraction_method_extract_correct_values(self):
        extractor = SchemaOneExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Descripton'
        test_data = [
            test_date,
            test_amount,
            test_indicator,
            '',
            '',
            '',
            '',
            '',
            '',
            test_description,
            '',
            '',
            ''
        ]

        transaction = extractor.extract(test_data)

        assert transaction.date == test_date
        assert transaction.amount == test_amount
        assert transaction.credit_or_debit == test_indicator
        assert transaction.description == test_description