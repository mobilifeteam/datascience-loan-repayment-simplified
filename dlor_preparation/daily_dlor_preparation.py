import pandas as pd
import numpy as np
from utilities import read_multiple_file
from logs_utilities import *
import logging
import sys

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def prepare_mapping_batch_payment(input_path, sheet_name):
    try:
        batch_df = pd.read_excel(input_path, sheet_name=sheet_name)
        log.info('Read Mapping Batch Payment Successfully')
    except FileNotFoundError as e:
        log.error('Mapping Batch Payment input file is not found({})'.format(input_path), exc_info=True)
        sys.exit(-1)

    batch_df.columns = ['contract_datetime', 'first_payment_date']

    batch_df = handle_incomplete_mapping_batch_payment(batch_df,['contract_datetime', 'first_payment_date'])

    batch_df['contract_datetime'] = pd.to_datetime(batch_df['contract_datetime'])

    return batch_df


def prepare_dlor_report(dlor_path, input_mapping_batch_df, column_name):

    dlor = read_multiple_file(dlor_path, "\t", header=None, directory=True)

    if is_dataframe_empty(dlor):
        log.error("DLOR Report is missing or cannot be read")
        sys.exit(-1)

    dlor.columns = column_name

    dlor = dlor[['transaction_customer_id', 'account_number', 'product_name', 'contract_datetime', 'contract_amount']]

    dlor = dlor.sort_values(by=['transaction_customer_id', 'contract_datetime'])

    dlor = dlor.drop_duplicates(subset=['transaction_customer_id'], keep='first')

    dlor_clean = dlor.replace("\\N", np.nan)

    sel_dlor = dlor_clean[(dlor_clean['product_name'] == 'MYCREDIT10000')
                          & (dlor_clean['contract_datetime'].notna())
                          & (dlor_clean['transaction_customer_id'].notna())
                          ]

    sel_dlor['contract_datetime'] = pd.to_datetime(sel_dlor['contract_datetime'], format="%d/%m/%Y", errors='coerce')

    if is_incorrect_datetime(sel_dlor, 'contract_datetime'):
        log.error("Unable to convert contract_datetime to proper format (%d/%m/%Y)")
        sys.exit(-1)

    sel_dlor['transaction_customer_id'] = sel_dlor['transaction_customer_id'].astype(int)
    sel_dlor['account_number'] = sel_dlor['account_number'].str.upper()

    sel_dlor = handle_incomplete_dlor_report(sel_dlor, ['transaction_customer_id', 'account_number', 'product_name', 'contract_datetime', 'contract_amount'])

    if is_lower_string(sel_dlor,'account_number'):
        log.error("DLOR Report cannot convert account_number to upper string")
        sys.exit(-1)

    dlor_with_mapping_batch = sel_dlor.merge(input_mapping_batch_df, how='left', on='contract_datetime')

    if is_dataframe_empty(dlor_with_mapping_batch):
        log.error("contract_datetime from the two input does not match")
        sys.exit(-1)

    return dlor_with_mapping_batch


def prepare_installment_datetime(dlor_view):

    dlor_view['installment_period'] = 0
    dlor_view['first_payment_date'] = pd.to_datetime(dlor_view['first_payment_date'])
    dlor_view['first_payment_month'] = dlor_view['first_payment_date'].dt.to_period('M')

    dlor_view = handle_incomplete_installment(dlor_view, ['installment_period'])

    return dlor_view


def combine_dlor(dlor_view, daily_update_dlor):
    selected_columns = ['account_number', 'product_name', 'contract_datetime', 'contract_amount', 'first_payment_date',
                        'first_payment_month', 'installment_period']
    dlor_report_with_selected_columns = dlor_view[selected_columns]

    today_dlor_customers = dlor_report_with_selected_columns[
        ~dlor_report_with_selected_columns['account_number'].isin(daily_update_dlor.account_number.unique())]
    all_dlor = pd.concat([daily_update_dlor, today_dlor_customers])

    return all_dlor


def prepare_dlor_daily(input_dlor_report_path,
                       dlor_report_columns,
                       daily_update_dlor_path,
                       input_mapping_batch_path,
                       sheet_name):

    daily_update_dlor = pd.read_csv(daily_update_dlor_path, sep='\t')

    input_mapping_batch = prepare_mapping_batch_payment(input_mapping_batch_path, sheet_name)

    dlor_report = prepare_dlor_report(input_dlor_report_path, input_mapping_batch, dlor_report_columns)

    dlor_report_with_details = prepare_installment_datetime(dlor_report)

    if is_incomplete_column(dlor_report_with_details, ['installment_period']):
        log.error("installment_period column is missing")
        sys.exit(-1)

    final_dlor_report = combine_dlor(dlor_report_with_details, daily_update_dlor)

    return final_dlor_report