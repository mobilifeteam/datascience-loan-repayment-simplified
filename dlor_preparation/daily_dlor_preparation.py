import pandas as pd
import numpy as np
from utilities import read_multiple_file


def prepare_mapping_batch_payment(input_path, sheet_name):

    batch_df = pd.read_excel(input_path, sheet_name=sheet_name)
    batch_df.columns = ['contract_datetime', 'first_payment_date']
    batch_df['contract_datetime'] = pd.to_datetime(batch_df['contract_datetime'])

    return batch_df


def prepare_dlor_report(dlor_path, input_mapping_batch_df, column_name):

    dlor = read_multiple_file(dlor_path, "\t", header=None, directory=True)
    dlor.columns = column_name

    dlor = dlor[['transaction_customer_id', 'account_number', 'product_name', 'contract_datetime', 'contract_amount']]

    dlor_clean = dlor.replace("\\N", np.nan)

    sel_dlor = dlor_clean[(dlor_clean['product_name'] == 'MYCREDIT10000')
                          & (dlor_clean['contract_datetime'].notna())
                          & (dlor_clean['transaction_customer_id'].notna())
                          ]

    sel_dlor['contract_datetime'] = pd.to_datetime(sel_dlor['contract_datetime'], format="%d/%m/%Y")
    sel_dlor['transaction_customer_id'] = sel_dlor['transaction_customer_id'].astype(int)
    sel_dlor['account_number'] = sel_dlor['account_number'].str.upper()

    dlor_with_mapping_batch = sel_dlor.merge(input_mapping_batch_df, how='left', on='contract_datetime')

    return dlor_with_mapping_batch


def prepare_installment_datetime(dlor_view):

    dlor_view['installment_period'] = 0
    dlor_view['first_payment_date'] = pd.to_datetime(dlor_view['first_payment_date'])
    dlor_view['first_payment_month'] = dlor_view['first_payment_date'].dt.to_period('M')

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

    final_dlor_report = combine_dlor(dlor_report_with_details, daily_update_dlor)

    return final_dlor_report