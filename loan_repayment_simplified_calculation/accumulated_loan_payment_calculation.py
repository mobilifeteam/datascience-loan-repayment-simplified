import pandas as pd


def prepare_accumulated_loan_payment(loan_payment_df, loan_details_df):

    loan_payment_view = loan_payment_df.groupby(['account_number', 'transaction_date']).agg(
        {'loan_payment_amount': 'sum', 'destination_ending_balance': 'min'
         ,'transaction_datetime' :'max'}).reset_index()
    loan_payment_view.sort_values(by=['account_number', 'transaction_date'], inplace=True)
    loan_payment_view.rename(columns={'transaction_datetime' :'latest_transaction_datetime'} ,inplace=True)

    loan_payment_view['cumsum_loan_payment_amount'] = loan_payment_view.groupby(['account_number'])[
        'loan_payment_amount'].cumsum()

    loan_payment_view = loan_payment_view[
        loan_payment_view['account_number'].isin(loan_details_df.account_number.unique())]

    loan_payment_with_dlor = loan_details_df.merge(loan_payment_view, on='account_number', how='right')
    loan_payment_with_dlor['latest_transaction_date'] = loan_payment_with_dlor['transaction_date']

    return loan_payment_with_dlor


def prepare_accumulated_payment_for_missed_current_payments(loan_payment_df, today_date):
    account_with_payment_history = loan_payment_df[
        loan_payment_df['transaction_date'] == today_date].account_number.unique()

    no_current_payment = loan_payment_df[~loan_payment_df['account_number'].isin(account_with_payment_history)]
    no_current_payment = no_current_payment.drop_duplicates(subset=['account_number'], keep='last')
    no_current_payment['latest_transaction_date'] = no_current_payment['transaction_date']
    no_current_payment['transaction_date'] = today_date

    combined_loan_payment = pd.concat([loan_payment_df, no_current_payment])

    return combined_loan_payment


def prepare_accumulated_payment_for_missed_all_payments(loan_payment_df, loan_details_df, today_date):

    loan_with_no_payment = loan_details_df[
        ~loan_details_df['account_number'].isin(loan_payment_df.account_number.unique())]
    loan_with_no_payment['latest_transaction_datetime'] = np.nan
    loan_with_no_payment['latest_transaction_date'] = np.nan
    loan_with_no_payment['transaction_date'] = today_date
    loan_with_no_payment['loan_payment_amount'] = 0
    loan_with_no_payment['cumsum_loan_payment_amount'] = 0
    loan_with_no_payment['destination_ending_balance'] = loan_with_no_payment['contract_amount']

    combined_loan_payment = pd.concat([loan_payment_df, loan_with_no_payment])
    return combined_loan_payment


def accumulated_loan_payment_calculation(loan_payment_df, loan_details_df, today_date):

    used_columns = ['transaction_customer_id', 'account_number', 'contract_date', 'debt_relief_month',
                    'contract_amount' ,'month_end_payment']

    accumulated_loan_payment = prepare_accumulated_loan_payment(loan_payment_df, loan_details_df)

    accumulated_loan_payment_with_current_payments = prepare_accumulated_payment_for_missed_current_payments(
        accumulated_loan_payment, today_date)

    final_accumulated_loan_payment = prepare_accumulated_payment_for_missed_all_payments(
        accumulated_loan_payment_with_current_payments, loan_details_df, today_date)

    final_accumulated_loan_payment.sort_values(by=['account_number' ,'transaction_date'] ,inplace=True)
    final_accumulated_loan_payment = final_accumulated_loan_payment.drop_duplicates(subset=['account_number']
                                                                                    ,keep='last')


    return final_accumulated_loan_payment