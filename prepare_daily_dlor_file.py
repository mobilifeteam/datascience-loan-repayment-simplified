import os
import sys
from dlor_preparation.daily_dlor_preparation import prepare_dlor_daily
from utilities import *
from pandas.tseries.offsets import DateOffset
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def main():

    execute_date = sys.argv[1]
    today_datetime = (pd.to_datetime(execute_date))
    today_date = today_datetime.to_period('D')

    previous_2_days_datetime = (today_datetime - DateOffset(days=1))
    previous_2_days_date = previous_2_days_datetime.to_period('D')

    config_file = sys.argv[2]
    config = read_json(config_file)

    input_dlor_report_path = "{}{}/*".format(config['input_dlor_report_path'],
                                           today_date.strftime("%Y%m%d"))
    dlor_report_columns = config['dlor_report_columns']
    daily_update_dlor_path = "{}{}.tsv".format(config['daily_update_dlor_path'],previous_2_days_date.strftime("%Y%m%d"))
    input_mapping_batch_path = config['input_mapping_batch_path']
    sheet_name = config['sheet_name']
    input_removed_account_path = config['input_removed_account_path']
    output_path = "{}{}.tsv".format(config['output_path'],today_date.strftime("%Y%m%d"))

    log.info("Daily DLOR Preparation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    dlor_daily = prepare_dlor_daily(input_dlor_report_path,
                       dlor_report_columns,
                       daily_update_dlor_path,
                       input_mapping_batch_path,
                        input_removed_account_path,
                       sheet_name)

    dlor_daily.to_csv(output_path, sep='\t', index=False)

    log.info("Daily DLOR Preparation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

if __name__ == "__main__":
    main()