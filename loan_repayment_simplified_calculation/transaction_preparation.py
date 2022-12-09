import pandas as pd
from utilities import read_multiple_file
from datetime import datetime
from logs_utilities import *
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")

def prepare_loan_payment_transactions(loan_payment_path,
                                      txn_columns):

    column_type_dict = {'account_number': str,
                        'destination_ending_balance': float,
                        'loan_principal_amount': float,
                        'loan_interest_amount': float}

    transaction_columns = ['transaction_id', 'transaction_customer_id', 'transaction_type', 'transaction_datetime',
                        'destination_acct_no', 'destination_ending_balance', 'loan_principal_amount',
                        'loan_interest_amount']

    log.info("Transaction Preparation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    loan_payment = read_multiple_file(loan_payment_path, "\t", None, directory=True)

    if is_dataframe_empty(loan_payment):
        log.error("Loan payment is missing or cannot be read")
        sys.exit(-1)

    loan_payment.columns = txn_columns

    if is_incomplete_column(loan_payment, transaction_columns):
        log.error("Loan payment required column does not exist")
        sys.exit(-1)

    loan_payment = handle_incomplete_transactions(loan_payment, transaction_columns)

    if is_wrong_data_format(loan_payment, "loan_principal_amount"):
        log.error("loan_principal_amount contains wrong data format")
        sys.exit(-1)

    if is_wrong_data_format(loan_payment, "loan_interest_amount"):
        log.error("loan_interest_amount contains wrong data format")
        sys.exit(-1)

    loan_payment.rename(columns={'destination_acct_no': 'account_number'}, inplace=True)

    loan_payment = loan_payment.astype(column_type_dict)

    loan_payment['loan_payment_amount'] = loan_payment['loan_principal_amount'] + loan_payment['loan_interest_amount']

    loan_payment["transaction_datetime"] = pd.to_datetime(loan_payment["transaction_datetime"])
    loan_payment["transaction_date"] = loan_payment["transaction_datetime"].dt.to_period('D')

    log.info("Transaction Preparation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    return loan_payment