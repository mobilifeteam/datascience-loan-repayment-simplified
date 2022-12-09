import pandas as pd
from utilities import read_multiple_file
from logs_utilities import *
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")

def prepare_loan_product_details(path, product_name):
    loan_product_details = pd.read_json(path, orient='index')
    loan_product_details = loan_product_details[loan_product_details['product_name'] == product_name]

    return loan_product_details


def prepare_dlor(daily_dlor_path,
                 header,
                 daily_dlor_columns,
                 product_name):
    column_type_dict = {'account_number': str, 'contract_amount': float}

    daily_dlor = read_multiple_file(daily_dlor_path, "\t", header, directory=False)

    daily_dlor = daily_dlor[daily_dlor_columns]

    daily_dlor = daily_dlor[daily_dlor['product_name'] == product_name]

    daily_dlor = daily_dlor.astype(column_type_dict)

    daily_dlor['contract_datetime'] = pd.to_datetime(daily_dlor['contract_datetime'])
    daily_dlor["contract_month"] = daily_dlor["contract_datetime"].dt.to_period('M')

    daily_dlor['first_payment_date'] = pd.to_datetime(daily_dlor['first_payment_date'])

    return daily_dlor


def prepare_dlor_with_loan_product_details(loan_product_details_path,
                                           daily_dlor_path,
                                           product_name):
    daily_dlor_columns = ['account_number', 'product_name', 'contract_datetime', 'contract_amount',
                          'first_payment_date', 'first_payment_month', 'installment_period', 'actual_payment_date',
                          'latest_actual_payment_date']

    log.info("Prepare dlor with loan product details (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    loan_product_details = prepare_loan_product_details(loan_product_details_path, product_name)

    daily_dlor = prepare_dlor(daily_dlor_path, 0, daily_dlor_columns, product_name)

    dlor_with_loan_product_details = loan_product_details.merge(daily_dlor, on='product_name', how='right')

    if is_dataframe_empty(dlor_with_loan_product_details):
        log.error("Loan Product Details and Daily DLOR have different product name")
        sys.exit(-1)

    log.info("Prepare dlor with loan product details (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    return dlor_with_loan_product_details