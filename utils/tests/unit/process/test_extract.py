from typing import List
import pytest

from pybudget.process.extract import (
    ChainOfResponsibilityTransactionExtractor,
    InvalidSchemaError,
    InvalidTransactionIndicatorError,
    SchemaFourExtractor,
    SchemaFiveExtractor,
    SchemaOneExtractor,
    SchemaThreeExtractor,
    SchemaTwoExtractor,
    TransactionExtractorPipeline,
    TransactionExtractorTemplate
)

class MockTransactionExtractorTemplate(TransactionExtractorTemplate):
    def _extraction_method(self, _: List[str]):
        pass

    def get_extractor_id(self):
        return -1


class TestChainOfResponsibilityTransactionExtractor:
    def test_unable_to_instantiate(self):
        with pytest.raises(TypeError):
            ChainOfResponsibilityTransactionExtractor()
    
    def test_unable_to_run_extract_transactions_method(self):
        test_data = [
            ['Test', 'Test', 'Test']
        ]
        with pytest.raises(TypeError):
            chain = ChainOfResponsibilityTransactionExtractor()
            chain.extract_transactions(test_data)


class TestTransactionExtractorPipeline:
    def test_does_not_throw_value_error_for_schema_one_data(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        try:
            pipeline.extract_transactions([test_data])[0]
        except InvalidSchemaError:
            assert False

    def test_correctly_extracts_schema_one_data(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        transaction = pipeline.extract_transactions([test_data])[0]

        assert transaction.date == test_date
        assert transaction.amount == test_amount
        assert transaction.credit_or_debit == test_indicator
        assert transaction.description == test_description

    def test_schema_one_data_uses_schema_one_extractor(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        extractor_id = pipeline.extract_transactions([test_data])[0].extractor_id
        expected_id = 1

        assert extractor_id == expected_id
    
    def test_correctly_extracts_schema_two_data(self):
        pipeline = TransactionExtractorPipeline()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        transaction = pipeline.extract_transactions([test_data])[0]

        assert transaction.date == test_date
        assert transaction.amount == test_amount
        assert transaction.credit_or_debit == test_indicator
        assert transaction.description == test_description

    def test_schema_two_data_uses_schema_two_extractor(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        extractor_id = pipeline.extract_transactions([test_data])[0].extractor_id
        expected_id = 2

        assert extractor_id == expected_id

    def test_does_not_throw_invalid_schema_error_for_schema_two_data(self):
        pipeline = TransactionExtractorPipeline()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        try:
            pipeline.extract_transactions([test_data])[0]
        except InvalidSchemaError:
            assert False
    
    def test_correctly_extracts_schema_three_data(self):
        pipeline = TransactionExtractorPipeline()

        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        transaction = pipeline.extract_transactions([test_data])[0]
        expected_amount = '666.0'

        assert transaction.date == test_date
        assert transaction.amount == expected_amount
        assert transaction.description == test_description

    def test_does_not_throw_invalid_schema_error_for_schema_three_data(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        try:
            pipeline.extract_transactions([test_data])[0]
        except InvalidSchemaError:
            assert False

    def test_schema_three_data_uses_schema_three_extractor(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        extractor_id = pipeline.extract_transactions([test_data])[0].extractor_id
        expected_id = 3

        assert extractor_id == expected_id
    
    def test_correctly_extracts_schema_four_data(self):
        pipeline = TransactionExtractorPipeline()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        transaction = pipeline.extract_transactions([test_data])[0]

        assert transaction.date == test_date
        assert transaction.amount == test_amount
        assert transaction.description == test_description

    def test_does_not_throw_invalid_schema_error_for_schema_four_data(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        try:
            pipeline.extract_transactions([test_data])[0]
        except InvalidSchemaError:
            assert False

    def test_schema_four_data_uses_schema_four_extractor(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        extractor_id = pipeline.extract_transactions([test_data])[0].extractor_id
        expected_id = 4

        assert extractor_id == expected_id

    def test_correctly_extracts_schema_five_data(self):
        pipeline = TransactionExtractorPipeline()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        transaction = pipeline.extract_transactions([test_data])[0]
        expected_amount = '-666.0'

        assert transaction.date == test_date
        assert transaction.amount == expected_amount
        assert transaction.description == test_description

    def test_does_not_throw_invalid_schema_error_for_schema_five_data(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        try:
            pipeline.extract_transactions([test_data])[0]
        except InvalidSchemaError:
            assert False

    def test_schema_five_data_uses_schema_five_extractor(self):
        pipeline = TransactionExtractorPipeline()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        extractor_id = pipeline.extract_transactions([test_data])[0].extractor_id
        expected_id = 5

        assert extractor_id == expected_id


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

        with pytest.raises(InvalidTransactionIndicatorError):
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
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        extractor.extract(test_data)

    def test_extraction_method_extract_correct_values(self):
        extractor = SchemaOneExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

    def test_extraction_method_throws_invalid_transaction_indicator_error_if_input_for_schema_two(self):
        extractor = SchemaOneExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        with pytest.raises(InvalidTransactionIndicatorError):
            extractor.extract(test_data)
    
    def test_extraction_method_throws_value_error_if_input_for_schema_three(self):
        extractor = SchemaOneExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_four(self):
        extractor = SchemaOneExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_five(self):
        extractor = SchemaOneExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)


class TestSchemaTwoExtractor:
    def test_extraction_method_succeeds_with_no_errors(self):
        extractor = SchemaTwoExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        extractor.extract(test_data)

    def test_extraction_method_extract_correct_values(self):
        extractor = SchemaTwoExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        transaction = extractor.extract(test_data)

        assert transaction.date == test_date
        assert transaction.amount == test_amount
        assert transaction.credit_or_debit == test_indicator
        assert transaction.description == test_description

    def test_extraction_method_throws_invalid_transaction_indicator_error_if_input_for_schema_one(self):
        extractor = SchemaTwoExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        with pytest.raises(InvalidTransactionIndicatorError):
            extractor.extract(test_data)
    
    def test_extraction_method_throws_value_error_if_input_for_schema_three(self):
        extractor = SchemaTwoExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_four(self):
        extractor = SchemaTwoExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_five(self):
        extractor = SchemaTwoExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)


class TestSchemaThreeExtractor:
    def test_extraction_method_succeeds_with_no_errors(self):
        extractor = SchemaThreeExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        extractor.extract(test_data)

    def test_extraction_method_extract_correct_values(self):
        extractor = SchemaThreeExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        transaction = extractor.extract(test_data)

        assert transaction.date == test_date
        assert transaction.amount == f'-{test_amount}'
        assert transaction.description == test_description

    def test_extraction_method_correctly_converts_negative_amount(self):
        extractor = SchemaThreeExtractor()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        transaction = extractor.extract(test_data)

        expected_amount = '666.0'
        assert transaction.amount == expected_amount

    def test_extraction_method_correctly_infers_debit_indicator(self):
        extractor = SchemaThreeExtractor()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        transaction = extractor.extract(test_data)
        expected_indicator = 'Debit'

        assert transaction.credit_or_debit == expected_indicator

    def test_extraction_method_correctly_infers_credit_indicator(self):
        extractor = SchemaThreeExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        transaction = extractor.extract(test_data)
        expected_indicator = 'Credit'

        assert transaction.credit_or_debit == expected_indicator

    def test_extraction_method_throws_value_error_if_input_for_schema_one(self):
        extractor = SchemaThreeExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        with pytest.raises(ValueError):
            extractor.extract(test_data)
    
    def test_extraction_method_throws_value_error_if_input_for_schema_two(self):
        extractor = SchemaThreeExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_four(self):
        extractor = SchemaThreeExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_five(self):
        extractor = SchemaThreeExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)


class TestSchemaFourExtractor:
    def test_extraction_method_succeeds_with_no_errors(self):
        extractor = SchemaFourExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        extractor.extract(test_data)

    def test_extraction_method_extract_correct_values(self):
        extractor = SchemaFourExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        transaction = extractor.extract(test_data)

        assert transaction.date == test_date
        assert transaction.amount == test_amount
        assert transaction.description == test_description

    def test_extraction_method_correctly_converts_negative_amount(self):
        extractor = SchemaFourExtractor()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        transaction = extractor.extract(test_data)

        expected_amount = '-666.0'
        assert transaction.amount == expected_amount

    def test_extraction_method_correctly_infers_debit_indicator(self):
        extractor = SchemaFourExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        transaction = extractor.extract(test_data)
        expected_indicator = 'Debit'

        assert transaction.credit_or_debit == expected_indicator

    def test_extraction_method_correctly_infers_credit_indicator(self):
        extractor = SchemaFourExtractor()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        transaction = extractor.extract(test_data)
        expected_indicator = 'Credit'

        assert transaction.credit_or_debit == expected_indicator

    def test_extraction_method_throws_value_error_if_input_for_schema_one(self):
        extractor = SchemaFourExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        with pytest.raises(ValueError):
            extractor.extract(test_data)
    
    def test_extraction_method_throws_value_error_if_input_for_schema_two(self):
        extractor = SchemaFourExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_three(self):
        extractor = SchemaFourExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_five(self):
        extractor = SchemaFourExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

class TestSchemaFiveExtractor:
    def test_extraction_method_succeeds_with_no_errors(self):
        extractor = SchemaFiveExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        extractor.extract(test_data)

    def test_extraction_method_extract_correct_values(self):
        extractor = SchemaFiveExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        transaction = extractor.extract(test_data)
        expected_amount = '-666.0'

        assert transaction.date == test_date
        assert transaction.amount == expected_amount
        assert transaction.description == test_description

    def test_extraction_method_correctly_converts_negative_amount(self):
        extractor = SchemaFiveExtractor()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        transaction = extractor.extract(test_data)

        expected_amount = '666.0'
        assert transaction.amount == expected_amount

    def test_extraction_method_correctly_infers_debit_indicator(self):
        extractor = SchemaFiveExtractor()
        test_date = '08/09/2024'
        test_amount = '-666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        transaction = extractor.extract(test_data)
        expected_indicator = 'Debit'

        assert transaction.credit_or_debit == expected_indicator

    def test_extraction_method_correctly_infers_credit_indicator(self):
        extractor = SchemaFiveExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            '',
            test_amount,
            '',
            ''
        ]

        transaction = extractor.extract(test_data)
        expected_indicator = 'Credit'

        assert transaction.credit_or_debit == expected_indicator

    def test_extraction_method_throws_value_error_if_input_for_schema_one(self):
        extractor = SchemaFiveExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
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

        with pytest.raises(ValueError):
            extractor.extract(test_data)
    
    def test_extraction_method_throws_value_error_if_input_for_schema_two(self):
        extractor = SchemaFiveExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_indicator = 'Debit'
        test_description = 'Description'
        test_data = [
            '',
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
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_three(self):
        extractor = SchemaFiveExtractor()
        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            '',
            test_description,
            '',
            '',
            test_amount,
            ''
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)

    def test_extraction_method_throws_value_error_if_input_for_schema_four(self):
        extractor = SchemaFiveExtractor()

        test_date = '08/09/2024'
        test_amount = '666.0'
        test_description = 'Description'
        test_data = [
            test_date,
            test_description,
            test_amount
        ]

        with pytest.raises(ValueError):
            extractor.extract(test_data)