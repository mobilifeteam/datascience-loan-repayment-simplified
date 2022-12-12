import pandas as pd
from functools import reduce
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def agg_all_account(loan_payment_view):
    all_account_df = loan_payment_view.groupby(['product_name_th']).agg({'account_number': 'count'}).reset_index()
    all_account_df.columns = ['product_name_th', 'all_account']

    return all_account_df


def agg_opened_closed_status(loan_payment_view):
    open_closed_df = pd.crosstab(loan_payment_view['product_name_th'],
                                 loan_payment_view['opened_closed_status']).reset_index()

    return open_closed_df


def agg_due_date_payment(loan_payment_view):
    filtered_due_date = loan_payment_view[loan_payment_view['opened_closed_status'] == 'opened_account']
    due_date_df = pd.crosstab(filtered_due_date['product_name_th'],
                              filtered_due_date['due_date_payment_type']).reset_index()

    return due_date_df


def agg_installment(loan_payment_view):
    filtered_installment = loan_payment_view[(loan_payment_view['opened_closed_status'] == 'opened_account') &
                                             (loan_payment_view[
                                                  'due_date_payment_type'] == 'opened_account_due_payment')]
    installment_df = pd.crosstab(filtered_installment['product_name_th'],
                                 filtered_installment['installment_label']).reset_index()

    return installment_df


def agg_installment_with_payment_method(loan_payment_view):
    filtered_installment = loan_payment_view[(loan_payment_view['opened_closed_status'] == 'opened_account') &
                                             (loan_payment_view[
                                                  'due_date_payment_type'] == 'opened_account_due_payment')]
    installment_with_payment_method = pd.crosstab(filtered_installment['product_name_th'],
                                                  filtered_installment['final_installment_label']).reset_index()

    return installment_with_payment_method


def agg_total_installment(loan_payment_view):
    filtered_installment = loan_payment_view[(loan_payment_view['opened_closed_status'] == 'opened_account') &
                                             (loan_payment_view[
                                                  'due_date_payment_type'] == 'opened_account_due_payment')]
    total_installment_df = pd.crosstab(index=filtered_installment['product_name_th'],
                                       columns=filtered_installment['installment_label'],
                                       values=filtered_installment.cumsum_loan_payment_amount, aggfunc=sum)
    total_installment_df = total_installment_df.add_prefix('total_amount_').reset_index()

    return total_installment_df


def agg_total_installment_with_payment_method(loan_payment_view):
    filtered_installment = loan_payment_view[(loan_payment_view['opened_closed_status'] == 'opened_account') &
                                             (loan_payment_view[
                                                  'due_date_payment_type'] == 'opened_account_due_payment')]
    total_final_installment_df = pd.crosstab(index=filtered_installment['product_name_th'],
                                             columns=filtered_installment['final_installment_label'],
                                             values=filtered_installment.cumsum_loan_payment_amount, aggfunc=sum)
    total_final_installment_df = total_final_installment_df.add_prefix('total_amount_').reset_index()

    return total_final_installment_df


def check_column_name(df):
    all_columns = ['product_name_th',
                   'date',
                   'all_account',
                   'opened_account',
                   'opened_account_due_payment',
                   'closed_account',
                   'payment_morethan_installment_account',
                   'self_payment_morethan_installment_account',
                   'auto_deduct_payment_morethan_installment_account',
                   'payment_equal_installment_account',
                   'self_payment_equal_installment_account',
                   'auto_deduct_payment_equal_installment_account',
                   'payment_lessthan_installment_account',
                   'self_payment_lessthan_installment_account',
                   'auto_deduct_payment_lessthan_installment_account',
                   'not_payment_account',
                   'not_due_payment_account',
                   'date_payment_schedule',
                   'total_amount_payment_morethan_installment_account',
                   'total_amount_self_payment_morethan_installment_account',
                   'total_amount_auto_deduct_payment_morethan_installment_account',
                   'total_amount_payment_equal_installment_account',
                   'total_amount_self_payment_equal_installment_account',
                   'total_amount_auto_deduct_payment_equal_installment_account',
                   'total_amount_payment_lessthan_installment_account',
                   'total_amount_self_payment_lessthan_installment_account',
                   'total_amount_auto_deduct_payment_lessthan_installment_account']

    for column in all_columns:
        if column not in list(df.columns):
            print(column)
            df[column] = 0

    return df[all_columns]


def update_dlor_report(loan_payment_view):
    column_name = ['account_number', 'product_name', 'contract_datetime', 'contract_amount', 'first_payment_date',
                   'first_payment_month', 'actual_payment_date', 'latest_actual_payment_date', 'installment_period']

    loan_payment_view['actual_payment_date'] = pd.to_datetime(loan_payment_view['actual_payment_date'])

    # loan_payment_view['latest_actual_payment_date'] = loan_payment_view['actual_payment_date']  # Add

    loan_payment_view['latest_actual_payment_date'] = loan_payment_view.apply(
        lambda row: row['latest_actual_payment_date'] if row['today_date'] < row['actual_payment_date']
        else row['actual_payment_date'], axis=1)

    loan_payment_view['actual_payment_date'] = loan_payment_view.apply(
        lambda row: row['latest_actual_payment_date'] if row['today_date'] < row['actual_payment_date']
        else row['next_payment_date'], axis=1)

    return loan_payment_view[column_name]


def aggregate_loan_repayment_simplified(loan_payment_view, today_date):

    log.info("Loan Repayment Simplified Aggregation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    function_list = [agg_all_account, agg_opened_closed_status, agg_due_date_payment, agg_installment,
                     agg_installment_with_payment_method,
                     agg_total_installment, agg_total_installment_with_payment_method]
    result_df_list = []

    for function_name in function_list:
        agg_result = function_name(loan_payment_view)
        result_df_list.append(agg_result)

    agg_result_final = reduce(lambda left, right: pd.merge(left, right, on='product_name_th', how='left'),
                              result_df_list)

    agg_result_final['date'] = today_date

    date_payment_schedule = loan_payment_view[loan_payment_view['installment_period'] != 0].actual_payment_date.max()
    agg_result_final['date_payment_schedule'] = date_payment_schedule

    agg_result_final = check_column_name(agg_result_final)
    agg_result_final.rename(columns={'product_name_th': 'product_name'}, inplace=True)

    dlor_daily_update = update_dlor_report(loan_payment_view)

    log.info("Loan Repayment Simplified Aggregation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    return agg_result_final, dlor_daily_update
