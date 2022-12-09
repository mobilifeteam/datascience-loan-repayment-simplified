from utilities import read_multiple_file
import logging
from datetime import datetime
from logs_utilities import *
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def concat_loan_repayment(input_path, output_path):

    log.info("Loan Repayment Simplified Concatenation (Start): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))

    all_result = read_multiple_file(input_path, "\t", header=0, directory=True)

    if is_dataframe_empty(all_result):
        log.error("Cannot concat loan repayment simplified due to missing file or missing path")
        sys.exit(-1)

    all_result.to_csv(output_path, sep='\t' ,index=False)

    log.info("Loan Repayment Simplified Concatenation (Finish): {}".format(datetime.now().strftime("%d/%m/%Y %H:%M:%S")))