import pandas as pd


def prepare_loan_payment_transactions(loan_payment,
                                      txn_columns):

    column_type_dict = {'account_number': str,
                        'destination_ending_balance': float,
                        'loan_principal_amount': float,
                        'loan_interest_amount': float}

    transaction_columns = ['transaction_id', 'transaction_customer_id', 'transaction_type', 'transaction_datetime',
                        'destination_acct_no', 'destination_ending_balance', 'loan_principal_amount',
                        'loan_interest_amount']

    loan_payment.rename(columns={'destination_acct_no': 'account_number'}, inplace=True)

    loan_payment = loan_payment.astype(column_type_dict)

    loan_payment['loan_payment_amount'] = loan_payment['loan_principal_amount'] + loan_payment['loan_interest_amount']

    loan_payment["transaction_datetime"] = pd.to_datetime(loan_payment["transaction_datetime"])
    loan_payment["transaction_date"] = loan_payment["transaction_datetime"].dt.to_period('D')

    return loan_payment