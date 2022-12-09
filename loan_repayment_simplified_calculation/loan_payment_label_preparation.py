import pandas as pd
from datetime import datetime
import logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def define_installment_label(loan_payment_view):

    loan_payment_view['contract_amount'] = round(loan_payment_view['contract_amount'].astype(float), 2)
    loan_payment_view.loc[((loan_payment_view['total_required_installment']) < (
    loan_payment_view['cumsum_loan_payment_amount'])), 'installment_label'] = 'payment_morethan_installment_account'
    loan_payment_view.loc[(loan_payment_view['total_required_installment']) == (
    loan_payment_view['cumsum_loan_payment_amount']), 'installment_label'] = 'payment_equal_installment_account'
    loan_payment_view.loc[(loan_payment_view['total_required_installment']) > (
    loan_payment_view['cumsum_loan_payment_amount']), 'installment_label'] = 'payment_lessthan_installment_account'
    loan_payment_view.loc[(loan_payment_view['cumsum_loan_payment_amount'] == 0) & (
                loan_payment_view['installment_period'] > 0), 'installment_label'] = 'not_payment_account'
    loan_payment_view.loc[(loan_payment_view['cumsum_loan_payment_amount'] == 0) & (
                loan_payment_view['installment_period'] == 0), 'installment_label'] = 'not_due_payment_account'
    loan_payment_view.loc[(loan_payment_view['contract_amount']) <= (
    loan_payment_view['cumsum_loan_payment_amount']), 'installment_label'] = 'closed_account'

    return loan_payment_view


def define_opened_closed_status(loan_payment_view):
    loan_payment_view.loc[
        loan_payment_view['installment_label'] == 'closed_account', 'opened_closed_status'] = 'closed_account'
    loan_payment_view.loc[
        loan_payment_view['installment_label'] != 'closed_account', 'opened_closed_status'] = 'opened_account'

    return loan_payment_view


def define_due_date_payment(loan_payment_view):
    loan_payment_view.loc[
        loan_payment_view['installment_period'] == 0, 'due_date_payment_type'] = 'not_due_payment_account'
    loan_payment_view.loc[
        loan_payment_view['installment_period'] != 0, 'due_date_payment_type'] = 'opened_account_due_payment'

    return loan_payment_view


def define_loan_repayment_simplified(loan_payment_view):
    loan_payment_view.loc[(loan_payment_view['is_payment'] == 'auto-deduct') &
                          (loan_payment_view[
                               'installment_label'] == 'payment_morethan_installment_account'), 'final_installment_label'] = 'auto_deduct_payment_morethan_installment_account'
    loan_payment_view.loc[(loan_payment_view['is_payment'] == 'self-payment') &
                          (loan_payment_view[
                               'installment_label'] == 'payment_morethan_installment_account'), 'final_installment_label'] = 'self_payment_morethan_installment_account'
    loan_payment_view.loc[(loan_payment_view['is_payment'] == 'auto-deduct') &
                          (loan_payment_view[
                               'installment_label'] == 'payment_equal_installment_account'), 'final_installment_label'] = 'auto_deduct_payment_equal_installment_account'
    loan_payment_view.loc[(loan_payment_view['is_payment'] == 'self-payment') &
                          (loan_payment_view[
                               'installment_label'] == 'payment_equal_installment_account'), 'final_installment_label'] = 'self_payment_equal_installment_account'
    loan_payment_view.loc[(loan_payment_view['is_payment'] == 'auto-deduct') &
                          (loan_payment_view[
                               'installment_label'] == 'payment_lessthan_installment_account'), 'final_installment_label'] = 'auto_deduct_payment_lessthan_installment_account'
    loan_payment_view.loc[(loan_payment_view['is_payment'] == 'self-payment') &
                          (loan_payment_view[
                               'installment_label'] == 'payment_lessthan_installment_account'), 'final_installment_label'] = 'self_payment_lessthan_installment_account'

    return loan_payment_view


def define_loan_payment(loan_payment_view):

    log.info("Loan Payment Label Preparation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    loan_payment_with_installment_label = define_installment_label(loan_payment_view)
    loan_payment_with_account_status = define_opened_closed_status(loan_payment_with_installment_label)
    loan_payment_with_due_date_payment = define_due_date_payment(loan_payment_with_account_status)
    loan_payment_with_final_label = define_loan_repayment_simplified(loan_payment_with_due_date_payment)

    log.info("Loan Payment Label Preparation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    return loan_payment_with_final_label