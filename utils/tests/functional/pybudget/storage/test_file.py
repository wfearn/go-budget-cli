import os

from pybudget import CSVFileManager

schema_one_csv_filename = 'schema_one.csv'
schema_one_headers = [
    'Transaction Date',
    'Post Date',
    'Description',
    'Category',
    'Type',
    'Amount',
    'Memo'
]
schema_one_test_data = [
    '07/03/2024,07/03/2024,AUTOMATIC PAYMENT - THANK,,Payment,1170.90,',
    '07/01/2024,07/02/2024,AMAZON MKTPL*RC85Q99C2,Shopping,Sale,-22.53,',
    '06/25/2024,06/26/2024,AMAZON MKTPL*RG17N2UZ1,Shopping,Sale,-21.76,',
    '06/23/2024,06/24/2024,AMAZON MKTPL*RG5SS4B52,Shopping,Sale,-60.65,',
    '06/24/2024,06/24/2024,AMAZON MKTPL*RG4X34QU1,Shopping,Sale,-19.78,'
]
schema_two_csv_filename = 'schema_two.csv'
schema_two_headers = [
    'Date',
    'Description',
    'Amount'
]
schema_two_test_data = [
    '07/06/2024,DD *DOORDASH CA,12.35',
    '07/06/2024,DD *DOORDASH CA,19.35',
    '07/03/2024,CAFE CA,3.23',
    '07/03/2024,CAFE             CA,2.44',
    '07/03/2024,GYRO CA,3.83',
    '07/02/2024,COFFEE CA,5.67',
    '07/02/2024,CAFE MD,2.92',
    '07/02/2024,DOORDASH FRANCISCO       CA,12.69'
]
schema_three_csv_filename = 'schema_three.csv'
schema_three_headers = [
    'Date',
    'Description',
    'Type',
    'Amount',
    'Current balance',
    'Status'
]
schema_three_test_data = [
    '2024-07-02,Cool Job,Direct Deposit,1.28,666.66,Posted',
    '2024-06-30,Interest earned,Interest Earned,1.23,3071.82,Posted',
    '2024-06-20,Gotta get that bugatti,Withdrawal,-100000.93,71.32,Posted',
]
schema_four_csv_filename = 'schema_four.csv'
schema_four_headers = [
    'Booking Date',
    'Amount',
    'Credit Debit Indicator',
    'type',
    'Type Group',
    'Reference',
    'Instructed Currency',
    'Currency Exchange Rate',
    'Instructed Amount',
    'Description',
    'Category',
    'Check Serial Number',
    'Card Ending'
]
schema_four_test_data = [
    '08/09/2024,420.00,Debit,ACH Debit,ACH Debit,,,,,Payment to Loan Bank,Credit Card Payments,200200020,',
    '08/09/2024,2.05,Debit,ACH Debit,ACH Debit,,,,,Payment to Utility Company,Utilities,000101000,',
    '08/09/2024,666.00,Debit,ACH Debit,ACH Debit,,,,,Transfer to Venmo,Transfers,020200100,',
    '08/09/2024,283.00,Credit,Credit,Credit,,,,,Transfer from Zelle,Transfers,,',
]

def setup_schema_one_csv_file():
    full_schema_one_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_one_csv_filename}'
    with open(full_schema_one_filename, 'w') as f:
        header_string = ','.join(schema_one_headers)
        print('Saving:', header_string)
        f.write(f'{header_string}\n')

        for datum in schema_one_test_data:
            print('Saving:', datum)
            f.write(f'{datum}\n')

def clean_schema_one_csv_file():
    full_schema_one_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_one_csv_filename}'
    if os.path.exists(full_schema_one_filename):
        os.remove(full_schema_one_filename)

def setup_schema_two_csv_file():
    full_schema_two_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_two_csv_filename}'
    with open(full_schema_two_filename, 'w') as f:
        header_string = ','.join(schema_two_headers)
        f.write(f'{header_string}\n')

        for datum in schema_two_test_data:
            f.write(f'{datum}\n')

def clean_schema_two_csv_file():
    full_schema_two_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_two_csv_filename}'
    if os.path.exists(full_schema_two_filename):
        os.remove(full_schema_two_filename)

def setup_schema_three_csv_file():
    full_schema_three_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_three_csv_filename}'
    with open(full_schema_three_filename, 'w') as f:
        header_string = ','.join(schema_three_headers)
        f.write(f'{header_string}\n')

        for datum in schema_three_test_data:
            f.write(f'{datum}\n')

def clean_schema_three_csv_file():
    full_schema_three_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_three_csv_filename}'
    if os.path.exists(full_schema_three_filename):
        os.remove(full_schema_three_filename)

def setup_schema_four_csv_file():
    full_schema_four_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_four_csv_filename}'
    with open(full_schema_four_filename, 'w') as f:
        header_string = ','.join(schema_four_headers)
        f.write(f'{header_string}\n')

        for datum in schema_four_test_data:
            f.write(f'{datum}\n')

def clean_schema_four_csv_file():
    full_schema_four_filename = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data/{schema_four_csv_filename}'
    if os.path.exists(full_schema_four_filename):
        os.remove(full_schema_four_filename)

def test_csv_file_manager_does_not_throw_error_on_schema_two_csv_file_read():
    setup_schema_two_csv_file()

    try:
        fm = CSVFileManager()
        fm.read(schema_two_csv_filename)

    finally:
        clean_schema_two_csv_file()

def test_csv_file_manager_does_not_throw_error_on_schema_three_csv_file_read():
    setup_schema_three_csv_file()

    try:
        fm = CSVFileManager()
        fm.read(schema_three_csv_filename)

    finally:
        clean_schema_three_csv_file()

def test_csv_file_manager_does_not_throw_error_on_schema_four_csv_file_read():
    setup_schema_four_csv_file()

    try:
        fm = CSVFileManager()
        fm.read(schema_four_csv_filename)

    finally:
        clean_schema_four_csv_file()

def test_csv_file_manager_reads_data_correctly_from_schema_one_csv_file():
    setup_schema_one_csv_file()

    try:
        fm = CSVFileManager()
        schema_one_data = fm.read(schema_one_csv_filename)

        for i, (
            transaction_date,
            post_date,
            description,
            category,
            type,
            amount,
            memo
        ) in enumerate(schema_one_data):
            data_string = ','.join([
                transaction_date,
                post_date,
                description,
                category,
                type,
                amount,
                memo
            ])
            if i == 0:
                true_header_string = ','.join(schema_one_headers)
                assert data_string == true_header_string
            else:
                true_data_string = schema_one_test_data[i - 1]
                assert data_string == true_data_string
    finally:
        clean_schema_one_csv_file()

def test_csv_file_manager_reads_data_correctly_from_schema_two_csv_file():
    setup_schema_two_csv_file()

    try:
        fm = CSVFileManager()
        schema_two_data = fm.read(schema_two_csv_filename)

        for i, (
            date,
            description,
            amount
        ) in enumerate(schema_two_data):
            data_string = ','.join([
                date,
                description,
                amount
            ])
            if i == 0:
                true_header_string = ','.join(schema_two_headers)
                assert data_string == true_header_string
            else:
                true_data_string = schema_two_test_data[i - 1]
                assert data_string == true_data_string
    finally:
        clean_schema_two_csv_file()

def test_csv_file_manager_reads_data_correctly_from_schema_three_csv_file():
    setup_schema_three_csv_file()

    try:
        fm = CSVFileManager()
        schema_three_data = fm.read(schema_three_csv_filename)

        for i, (
            date,
            description,
            type,
            amount,
            current_balance,
            status
        ) in enumerate(schema_three_data):
            data_string = ','.join([
                date,
                description,
                type,
                amount,
                current_balance,
                status
            ])
            if i == 0:
                true_header_string = ','.join(schema_three_headers)
                assert data_string == true_header_string
            else:
                true_data_string = schema_three_test_data[i - 1]
                assert data_string == true_data_string
    finally:
        clean_schema_three_csv_file()

def test_csv_file_manager_reads_data_correctly_from_schema_four_csv_file():
    setup_schema_four_csv_file()

    try:
        fm = CSVFileManager()
        schema_four_data = fm.read(schema_four_csv_filename)

        for i, (
            booking_date,
            amount,
            credit_debit_indicator,
            type,
            type_group,
            reference,
            instructed_currency,
            currency_exchange_rate,
            instructed_amount,
            description,
            category,
            check_serial_number,
            card_ending
        ) in enumerate(schema_four_data):
            data_string = ','.join([
                booking_date,
                amount,
                credit_debit_indicator,
                type,
                type_group,
                reference,
                instructed_currency,
                currency_exchange_rate,
                instructed_amount,
                description,
                category,
                check_serial_number,
                card_ending
            ])
            if i == 0:
                true_header_string = ','.join(schema_four_headers)
                assert data_string == true_header_string
            else:
                true_data_string = schema_four_test_data[i - 1]
                assert data_string == true_data_string
    finally:
        clean_schema_four_csv_file()

def test_csv_file_manager_writes_data_correctly_from_schema_one_csv_file():
    data_to_write = [','.join(schema_one_headers)] + schema_one_test_data

    try:
        fm = CSVFileManager()
        fm.write(data_to_write, schema_one_csv_filename)
        data = fm.read(schema_one_csv_filename)

        for i in range(len(data_to_write)):
            true_data = data_to_write[i]
            read_data = data[i]

            for (j, k) in zip(true_data, read_data):
                assert j == k
    finally:
        clean_schema_one_csv_file()

def test_csv_file_manager_writes_data_correctly_from_schema_two_csv_file():
    data_to_write = [','.join(schema_two_headers)] + schema_two_test_data

    try:
        fm = CSVFileManager()
        fm.write(data_to_write, schema_two_csv_filename)
        data = fm.read(schema_two_csv_filename)

        for i in range(len(data_to_write)):
            true_data = data_to_write[i]
            read_data = data[i]

            for (j, k) in zip(true_data, read_data):
                assert j == k
    finally:
        clean_schema_two_csv_file()

def test_csv_file_manager_writes_data_correctly_from_schema_four_csv_file():
    data_to_write = [','.join(schema_four_headers)] + schema_four_test_data

    try:
        fm = CSVFileManager()
        fm.write(data_to_write, schema_four_csv_filename)
        data = fm.read(schema_four_csv_filename)

        for i in range(len(data_to_write)):
            true_data = data_to_write[i]
            read_data = data[i]

            for (j, k) in zip(true_data, read_data):
                assert j == k
    finally:
        clean_schema_four_csv_file()

def test_csv_file_manager_writes_data_correctly_from_schema_three_csv_file():
    data_to_write = [','.join(schema_three_headers)] + schema_three_test_data

    try:
        fm = CSVFileManager()
        fm.write(data_to_write, schema_three_csv_filename)
        data = fm.read(schema_three_csv_filename)

        for i in range(len(data_to_write)):
            true_data = data_to_write[i]
            read_data = data[i]

            for (j, k) in zip(true_data, read_data):
                assert j == k
    finally:
        clean_schema_three_csv_file()

def test_csv_file_manager_writes_to_raw_data_directory():
    raw_data_directory = f'{os.getenv("SYSTEMDRIVE")}/.pybudget/data'
    assert not os.path.exists(f'{raw_data_directory}/{schema_one_csv_filename}')

    schema_one_data = [','.join(schema_one_headers)] + schema_one_test_data

    try:
        fm = CSVFileManager()
        fm.write(schema_one_data, schema_one_csv_filename)

        assert os.path.exists(f'{raw_data_directory}/{schema_one_csv_filename}')
    finally:
        clean_schema_one_csv_file()

def test_csv_file_manager_reads_from_non_default_directory_with_no_errors():
    non_default_directory = f'{os.getenv("SYSTEMDRIVE")}/dumbdir'
    assert not os.path.isdir(non_default_directory)
    filename = f'{non_default_directory}/{schema_one_csv_filename}'

    try:
        os.mkdir(non_default_directory)
        fm = CSVFileManager()
        fm.write(schema_one_test_data, filename)

        assert os.path.exists(filename)

    finally:
        os.remove(filename)
        os.rmdir(non_default_directory)
