from utilities import read_multiple_file


def concat_loan_repayment(input_path, output_path):

    all_result = read_multiple_file(input_path, "\t", header=0, directory=True)

    all_result.to_csv(output_path, sep='\t' ,index=False)