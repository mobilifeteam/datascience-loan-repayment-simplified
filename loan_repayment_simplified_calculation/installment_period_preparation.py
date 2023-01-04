import pandas as pd
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")
from datetime import datetime


def prepare_mapping_batch_payment(input_mapping_batch_payment_path, sheet_name):
    batch_df = pd.read_excel(input_mapping_batch_payment_path, sheet_name=sheet_name)
    batch_df['actual_payment_date'] = pd.to_datetime(batch_df['actual_payment_datetime']).dt.strftime('%Y-%m-%d')
    batch_df['batch_payment_date'] = pd.to_datetime(batch_df['batch_payment_datetime']).dt.strftime('%Y-%m-%d')
    batch_df['next_payment_date'] = pd.to_datetime(batch_df['next_payment_date']).dt.strftime('%Y-%m-%d')
    batch_df = batch_df[['actual_payment_date', 'batch_payment_date', 'next_payment_date']]
    batch_df['actual_payment_date'] = pd.to_datetime(batch_df['actual_payment_date'])
    batch_df['batch_payment_date'] = pd.to_datetime(batch_df['batch_payment_date'])
    batch_df['next_payment_date'] = pd.to_datetime(batch_df['next_payment_date'])

    return batch_df


def create_next_payment_date(dlor_df, batch_df):
    dlor_df['latest_actual_payment_date'] = pd.to_datetime(dlor_df['latest_actual_payment_date'])
    dlor_df['actual_payment_date'] = pd.to_datetime(dlor_df['actual_payment_date'])
    merge_df = dlor_df.merge(batch_df[['batch_payment_date', 'next_payment_date']], left_on=['actual_payment_date'],
                             right_on='batch_payment_date')
    merge_df = merge_df.drop(columns=['batch_payment_date'])

    return merge_df


# def create_next_payment_date(dlor_df, batch_df):
#
#     dlor_df['latest_actual_payment_date'] = pd.to_datetime(dlor_df['latest_actual_payment_date']).dt.to_period('D')
#     merge_df = dlor_df.merge(batch_df[['batch_payment_date', 'next_payment_date']],
#                              left_on=['latest_actual_payment_date'], right_on='batch_payment_date')
#     merge_df = merge_df.drop(columns=['batch_payment_date'])
#
#     return merge_df

def create_installment_period(dlor_df, today_datetime, batch_df):
    dlor_df['today_date'] = today_datetime.to_period('D')
    dlor_df['today_month'] = today_datetime.to_period('M')
    dlor_df['first_payment_date'] = pd.to_datetime(dlor_df['first_payment_date'])
    dlor_df['today_date'] = dlor_df['today_date'].dt.to_timestamp()
    dlor_df['actual_payment_date'] = pd.to_datetime(dlor_df['actual_payment_date'])

    dlor_df.loc[dlor_df['actual_payment_date'].isna(), 'actual_payment_date'] = dlor_df['first_payment_date']
    dlor_df.loc[dlor_df['latest_actual_payment_date'].isnull(), 'latest_actual_payment_date'] = dlor_df[
        'actual_payment_date']
    dlor_df = create_next_payment_date(dlor_df, batch_df)

    dlor_df['installment_period'] = dlor_df.apply(
        lambda row: row['installment_period'] if row['today_date'] < row['actual_payment_date']
        else row['installment_period'] + 1, axis=1)

    dlor_df['actual_payment_date'] = dlor_df.apply(
        lambda row: row['latest_actual_payment_date'] if row['today_date'] < row['actual_payment_date']
        else row['actual_payment_date'], axis=1)

    return dlor_df


# def create_installment_period(dlor_df, today_datetime, batch_df):
#
#     dlor_df['today_date'] = today_datetime.to_period('D')
#     dlor_df['today_month'] = today_datetime.to_period('M')
#     dlor_df['first_payment_date'] = pd.to_datetime(dlor_df['first_payment_date'])
#     dlor_df['today_date'] = dlor_df['today_date'].dt.to_timestamp()
#     dlor_df['actual_payment_date'] = pd.to_datetime(dlor_df['actual_payment_date'])
#
#     dlor_df.loc[dlor_df['actual_payment_date'].isna(), 'actual_payment_date'] = dlor_df['first_payment_date']
#     dlor_df.loc[dlor_df['latest_actual_payment_date'].isnull(), 'latest_actual_payment_date'] = dlor_df[
#         'actual_payment_date']
#     dlor_df = create_next_payment_date(dlor_df, batch_df)
#
#     dlor_df['installment_period'] = dlor_df.apply(
#         lambda row: row['installment_period'] if row['today_date'] < row['actual_payment_date']
#         else row['installment_period'] + 1, axis=1)
#
#     dlor_df['actual_payment_date'] = dlor_df.apply(
#         lambda row: row['latest_actual_payment_date'] if row['today_date'] < row['actual_payment_date']
#         else row['next_payment_date'], axis=1)
#
#     return dlor_df


def prepare_installment_period(dlor_with_loan_product_details_df,
                               today_datetime,
                               input_mapping_batch_path,
                               sheet_name):

    log.info("Installment Period Preparation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    input_mapping_batch = prepare_mapping_batch_payment(input_mapping_batch_path, sheet_name)

    updated_dlor = create_installment_period(dlor_with_loan_product_details_df, today_datetime, input_mapping_batch)

    log.info("Installment Period Preparation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    return updated_dlor