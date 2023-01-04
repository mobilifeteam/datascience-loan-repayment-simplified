from pandas.tseries.offsets import DateOffset
import pandas as pd
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def prepare_actual_payment_date(loan_payment_view):

    datetime_args = {"hours": 23, "minutes": 59, "second": 59}

    loan_payment_view['actual_payment_date'] = pd.to_datetime(loan_payment_view['actual_payment_date'])
    loan_payment_view['actual_payment_datetime'] = loan_payment_view.apply(
        lambda row: row['actual_payment_date'] + DateOffset(hours=22), axis=1)
    loan_payment_view['actual_payment_datetime_end'] = loan_payment_view.apply(
        lambda row: row['actual_payment_date'] + DateOffset(**datetime_args), axis=1)

    return loan_payment_view


# def prepare_actual_payment_date(loan_payment_view):
#
#     datetime_args = {"hours" :23, "minutes" :59, "second" :59}
#
#     loan_payment_view['actual_payment_date'] = loan_payment_view['actual_payment_date'].dt.to_timestamp()
#     loan_payment_view['actual_payment_datetime'] = loan_payment_view.apply(
#         lambda row: row['actual_payment_date'] + DateOffset(hours=22), axis=1)
#     loan_payment_view['actual_payment_datetime_end'] = loan_payment_view.apply(
#         lambda row: row['actual_payment_date'] + DateOffset(**datetime_args), axis=1)
#
#     return loan_payment_view


def create_loan_payment_label(loan_payment_view):

    loan_payment_view.loc[
        (loan_payment_view['latest_transaction_datetime'] < loan_payment_view['actual_payment_datetime']) & (
                    loan_payment_view['installment_period'] == 0), 'is_payment'] = 'self-payment'
    loan_payment_view.loc[(loan_payment_view['latest_transaction_datetime'].isna()) & (
                loan_payment_view['installment_period'] == 0), 'is_payment'] = 'not due-date'

    loan_payment_view.loc[
        (loan_payment_view['latest_transaction_datetime'] < loan_payment_view['actual_payment_datetime']) & (
                    loan_payment_view['installment_period'] > 0), 'is_payment'] = 'self-payment'
    loan_payment_view.loc[
        (loan_payment_view['latest_transaction_datetime'] >= loan_payment_view['actual_payment_datetime']) &
        (loan_payment_view['latest_transaction_datetime'] <= loan_payment_view['actual_payment_datetime_end']) & (
                    loan_payment_view['installment_period'] > 0), 'is_payment'] = 'auto-deduct'
    loan_payment_view.loc[
        (loan_payment_view['latest_transaction_datetime'] > loan_payment_view['actual_payment_datetime_end']) & (
                    loan_payment_view['installment_period'] > 0), 'is_payment'] = 'self-payment'
    loan_payment_view.loc[loan_payment_view['is_payment'].isna(), 'is_payment'] = 'no payment'

    return loan_payment_view


def create_total_payment(loan_payment_view):

    loan_payment_view["cumsum_loan_payment_amount"] = round(
        loan_payment_view["cumsum_loan_payment_amount"].astype(float), 2)

    loan_payment_view["total_required_installment"] = (
                loan_payment_view['loan_installment_amount'] * loan_payment_view['installment_period'])

    loan_payment_view["installment_period"] = loan_payment_view["installment_period"].astype(int)

    return loan_payment_view


def prepare_loan_payment_definition(loan_payment_view):

    log.info("Loan Payment Definition Preparation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    loan_payment_with_actual_datetime = prepare_actual_payment_date(loan_payment_view)
    loan_payment_with_payment_label = create_loan_payment_label(loan_payment_with_actual_datetime)
    loan_payment_with_required_installment = create_total_payment(loan_payment_with_payment_label)

    log.info("Loan Payment Definition Preparation (Finish: {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    return loan_payment_with_required_installment