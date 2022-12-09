import os
import sys
from utilities import *
from datetime import datetime
from pandas.tseries.offsets import DateOffset
from loan_repayment_simplified_calculation.dlor_with_loan_details_preparation import *
from loan_repayment_simplified_calculation.installment_period_preparation import *
from loan_repayment_simplified_calculation.transaction_preparation import *
from loan_repayment_simplified_calculation.accumulated_loan_payment_calculation import *
from loan_repayment_simplified_calculation.loan_payment_definition_preparation import *
from loan_repayment_simplified_calculation.loan_payment_label_preparation import *
from loan_repayment_simplified_calculation.loan_repayment_simplified_preparation import *
from loan_repayment_simplified_calculation.loan_repayment_simplified_concatenation import *

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")

def main():

    execute_date = sys.argv[1]
    today_datetime = (pd.to_datetime(execute_date))
    today_date = today_datetime.to_period('D')

    config_file = sys.argv[2]
    config = read_json(config_file)

    product_name = config['product_name']
    input_mapping_batch_path = config['input_mapping_batch_path']
    sheet_name = config['sheet_name']
    loan_product_details_path = config['loan_product_details_path']
    daily_dlor_path = "{}{}.tsv".format(config['daily_dlor_path'],today_date.strftime("%Y%m%d"))
    txn_path = "{}{}/*".format(config['txn_path'],today_date.strftime("%Y%m%d"))
    txn_columns = config['txn_columns']
    loan_payment_simplified_output_path = "{}{}.tsv".format(config['loan_payment_simplified_output_path'],today_date.strftime("%Y%m%d"))
    dlor_daily_output_path = "{}{}.tsv".format(config['dlor_daily_output_path'], today_date.strftime("%Y%m%d"))
    multiple_loan_simplified_path = config['multiple_simplified_path']
    multiple_loan_simplified_output_path = "{}{}.tsv".format(config['multiple_loan_simplified_output_path'],today_date.strftime("%Y%m%d"))

    log.info("Loan Repayment Simplified Calculation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    dlor_with_loan_product_details = prepare_dlor_with_loan_product_details(loan_product_details_path,
                                                                            daily_dlor_path,
                                                                            product_name)
    updated_dlor = prepare_installment_period(dlor_with_loan_product_details, today_datetime, input_mapping_batch_path,
                                              sheet_name)

    loan_payment = prepare_loan_payment_transactions(txn_path, txn_columns)

    accumulated_loan_payment = accumulated_loan_payment_calculation(loan_payment, updated_dlor, today_date)

    accumulated_loan_payment_with_definition = prepare_loan_payment_definition(accumulated_loan_payment)

    accumulated_loan_payment_with_label = define_loan_payment(accumulated_loan_payment_with_definition)

    agg_loan_repayment_simplified_result, dlor_daily_update = aggregate_loan_repayment_simplified(accumulated_loan_payment_with_label, today_date)

    agg_loan_repayment_simplified_result.to_csv(loan_payment_simplified_output_path, sep='\t', index=False)

    dlor_daily_update.to_csv(dlor_daily_output_path, sep='\t', index=False)

    concat_loan_repayment(multiple_loan_simplified_path, multiple_loan_simplified_output_path)

    log.info("Loan Repayment Simplified Calculation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))


if __name__ == "__main__":
    main()