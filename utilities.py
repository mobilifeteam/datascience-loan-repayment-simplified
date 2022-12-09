import pandas as pd
import glob
import logging
import json

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Loan_Repayment_Simplified")


def read_multiple_file(file_path, separate, header=None, directory=True):
    final_df = pd.DataFrame()
    if directory:
        filenames = glob.glob(file_path)
        filenames.sort()
        for file in filenames[:]:
            df = pd.read_csv(file,sep=separate, header=header)
            final_df = final_df.append(df, ignore_index=False)
            del df
    else:
        try:
            final_df = pd.read_csv(file_path,sep=separate, header=header)
        except FileNotFoundError as e:
            log.error('The whole input file is missing or cannot be read ({})'.format(file_path), exc_info=True)
    return final_df


def read_json(file_path):

    with open(file_path, 'r') as f:
        data = json.load(f)

    return data
