import logging
import sys
import pandas as pd
import numpy as np

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("LOAN_REPAYMENT_SIMPLIFIED")


def create_warning_log(warning_pattern, log_string_list):
    case_name = log_string_list[0]
    example = log_string_list[1].to_dict()
    number_of_rows_before = log_string_list[2]
    number_of_rows_after = log_string_list[3]
    warning_pattern = warning_pattern.format(case_name, number_of_rows_before, example, number_of_rows_after)
    log.warning(warning_pattern)


def is_dataframe_empty(txn_df):
    if len(txn_df)==0:
        return True
    else:
        return False


def is_lower_string(df, column):

    if len(df[df[column].str.islower()])!=0:
        return True
    else:
        return False


def is_incorrect_datetime(df, column):
    if len(df[df[column].isna()])!=0:
        return True
    else:
        return False


def is_incomplete_column(txn_df, required_columns_list):
    input_column = list(txn_df.columns)
    missing_column = []
    for column in required_columns_list:
        if column not in input_column:
            missing_column.append(column)
    if len(missing_column)!=0:
        return True
    else:
        return False


def is_wrong_data_format(df, column_name):
    df[column_name] = pd.to_numeric(df[column_name], errors='coerce')

    if len(df[df[column_name].isna()]) != 0:
        return True
    else:
        return False


def remove_duplicated_transactions(txn_df, column_name, warning_pattern):
    case = 'Some rows from input data have duplicated transaction_id'
    incomplete_df = txn_df[txn_df[column_name].duplicated()]

    if len(incomplete_df) != 0:
        number_of_rows_before = len(txn_df)
        txn_df.drop_duplicates(subset=[column_name], inplace=True)
        number_of_rows_after = len(txn_df)
        create_warning_log(warning_pattern, [case, incomplete_df[:1], number_of_rows_before, number_of_rows_after])

    return txn_df


def replace_with_correct_values(txn_df, input_string, output_string, warning_pattern):
    case = 'Some rows from input data contain {}'.format(input_string)
    incomplete_df = txn_df[txn_df.eq(input_string).any(axis=1)]

    if len(incomplete_df) != 0:
        number_of_rows_before = len(txn_df)
        txn_df = txn_df.replace(input_string, output_string)
        number_of_rows_after = len(txn_df)
        create_warning_log(warning_pattern, [case, incomplete_df[:1], number_of_rows_before, number_of_rows_after])

    return txn_df


def remove_missing_value(txn_df, required_columns, warning_pattern):

    case = 'Some rows from input data are missing or null (NA)'
    incomplete_df = txn_df[txn_df.isnull().any(axis=1)]

    if len(incomplete_df) != 0:
        number_of_rows_before = len(txn_df)
        txn_df.dropna(subset=required_columns, inplace=True)
        number_of_rows_after = len(txn_df)
        create_warning_log(warning_pattern, [case, incomplete_df[:1], number_of_rows_before, number_of_rows_after])

    return txn_df


def incorrect_data(txn_df, correct_data_query_string, incorrect_data_query_string, warning_pattern):

    case = 'Some rows from input data are not required'
    incomplete_df = txn_df.query(incorrect_data_query_string)

    if len(incomplete_df) != 0:
        number_of_rows_before = len(txn_df)
        txn_df = txn_df.query(correct_data_query_string)
        number_of_rows_after = len(txn_df)
        create_warning_log(warning_pattern, [case, incomplete_df[:1], number_of_rows_before, number_of_rows_after])

    return txn_df


def handle_incomplete_mapping_batch_payment(txn_df, required_columns):

    warning_pattern="Case:{} \n Number of rows before cleaned: {} \n Example:{} \n Number of rows after cleaned: {}"

    cleaned_txn = replace_with_correct_values(txn_df, "\\N", np.nan, warning_pattern)
    cleaned_txn = remove_missing_value(cleaned_txn, required_columns, warning_pattern)

    return cleaned_txn


def handle_incomplete_dlor_report(txn_df, required_columns):

    warning_pattern="Case:{} \n Number of rows before cleaned: {} \n Example:{} \n Number of rows after cleaned: {}"

    cleaned_txn = replace_with_correct_values(txn_df, "\\N", np.nan, warning_pattern)
    cleaned_txn = remove_missing_value(cleaned_txn, required_columns, warning_pattern)
    cleaned_txn = incorrect_data(cleaned_txn,'product_name == "MYCREDIT10000"', 'product_name != "MYCREDIT10000"' , warning_pattern)

    return cleaned_txn


def handle_incomplete_installment(txn_df, required_columns):

    warning_pattern = "Case:{} \n Number of rows before cleaned: {} \n Example:{} \n Number of rows after cleaned: {}"
    cleaned_txn = incorrect_data(txn_df, 'installment_period == 0', 'installment_period != 0', warning_pattern)
    cleaned_txn = remove_missing_value(cleaned_txn, required_columns, warning_pattern)

    return cleaned_txn


def handle_incomplete_transactions(txn_df, required_columns):

    warning_pattern="Case:{} \n Number of rows before cleaned: {} \n Example:{} \n Number of rows after cleaned: {}"
    cleaned_txn = remove_duplicated_transactions(txn_df, "transaction_id", warning_pattern)
    cleaned_txn = replace_with_correct_values(cleaned_txn, "\\N", np.nan, warning_pattern)
    cleaned_txn = remove_missing_value(cleaned_txn, required_columns, warning_pattern)
    cleaned_txn = incorrect_data(cleaned_txn,'transaction_type == "loan_payment"', 'transaction_type != "loan_payment"' , warning_pattern)

    return cleaned_txn
