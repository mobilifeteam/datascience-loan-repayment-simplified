# datascience-loan-repayment-simplified-research_zone-version

## Description
Loan Repayment Simplified Pipeline is the main pipeline for calculate changing of daily loan payment.

## Pipeline Components
There are 2 main components; `dlor_preparation` and `loan_repayment_simplified_calculation`
### 1. Daily DLOR Preparation
To obtain new dlor customers and update daily dlor file.

* Input data are `Daily update DLOR`, `Input mapping batch (Contract)`, `Input removed account number` and `DLOR report`.
* Output data is `Intermediate DLOR file`.

### 2. Loan Repayment Simplified Calculation
* To calculate daily loan repayment for each customers.
* To aggregate loan repayment result into single row for display on loan repayment dashboard.
* To create updated daily DLOR for DLOR Preparation process in the next day.
* Input data are `Input mapping batch (Payment)` and `Intermediate DLOR file`
* Output data are `Loan repayment simplified (Single row & date)`,`Loan repayment simplified (Multiple date)`  and `Daily update DLOR`

## How to run
1. Component files to run
   * Daily DLOR Preparation Configuration File 
   ```sh
     {"input_dlor_report_path":"/loan_repayment_dashboard/raw_data/dlor/dlor_report_",
     "dlor_report_columns":
     ["transaction_datetime",
     "transaction_customer_id",
     "dopa_status",
     "customer_identification_checking_result",
     "age",
     "occupation_code",
     "restriction_code",
     "blacklist_condition",
     "whitelist_condition",
     "informal_debt_re_structure_condition",
     "tdr_condition",
     "negative_file_condition",
     "customer_qualification_checking_result",
     "requested_amount",
     "ncb_e_consent",
     "ncb_score",
     "npl",
     "dscr",
     "loan_approval_result",
     "loan_approval_result_description",
     "write_off_condition",
     "approved_amount",
     "total_due_amount",
     "approved_product_name",
     "frl_account_status",
     "product_name",
     "credit_score_when_contract",
     "alternative_credit_score_version",
     "ncb_date",
     "subject_id",
     "ncb_score",
     "account_number",
     "ncb_account_status",
     "payment_pattern",
     "credit_limit",
     "account_type",
     "amount_owed",
     "installment_amount",
     "as_of_date",
     "ncb_ref",
     "contract_datetime",
     "contract_number",
     "sending_contract",
     "contract_amount",
     "disburse",
     "result_description"],
     "daily_update_dlor_path":"/loan_repayment_dashboard/dlor_prep/dlor_update_",
     "input_mapping_batch_path":"/loan_repayment_dashboard/20221202-Map_batch_payment_date.xlsx",
     "output_path":"/loan_repayment_dashboard/intermediate/intermediate_dlor_file_",
     "sheet_name":"contract",
     "input_removed_account_path":"/loan_repayment_dashboard/config/removed_account_number.json"
     }
     ```
   * Loan Repayment Simplified Calculation Configuration File
   ```sh
   {"product_name":"MYCREDIT10000",
   "input_mapping_batch_path":"/loan_repayment_dashboard/20221202-Map_batch_payment_date.xlsx",
   "sheet_name":"payment",
   "loan_product_details_path":"/loan_repayment_dashboard/loan_product_details_simplified.json",
   "daily_dlor_path":"/loan_repayment_dashboard/intermediate/intermediate_dlor_file_",
   "txn_path":"/loan_repayment_dashboard/raw_data/txn/txn_mycredit_",
   "txn_columns":["transaction_id", "transaction_customer_id", "transaction_amount", "transaction_datetime", "transaction_direction", "transaction_type", "destination_acct_product_type", "destination_acct_no", "destination_acct_type", "destination_acct_sub_type", "destination_acct_market_code","destination_ending_balance","loan_principal_amount","loan_interest_amount","loan_penalty_amount","loan_fee_amount"],
   "loan_payment_simplified_output_path":"/loan_repayment_dashboard/result/loan_repayment_simplified_",
   "dlor_daily_output_path":"/loan_repayment_dashboard/dlor_prep/dlor_update_",
   "multiple_simplified_path":"/loan_repayment_dashboard/result/*",
   "multiple_loan_simplified_output_path":"/loan_repayment_dashboard/loan_repayment_simplified/loan_repayment_simplified_"}
   ```
    * Removed Account Number File (JSON)
   ```sh
   [{"account_number": "40A8926E53D2A4AE08C72861A5A3AAD1"},
   {"account_number": "50523421287C8AD389F49E7E04475BAA"}]
   ```
   * Txn and DLOR shell script (.sh)
   ```sh
   #!/bin/bash
   useDateString="$(date -d "$(date) - 1 day" "+%Y-%m-%d")"
   useDateStringNew="$(date -d "$(date) - 1 day" "+%Y%m%d")"
   
   DLOR_SCRIPTS_FILE_NAME=/data/store/view/loan_repayment_dashboard/scripts/dlor/get_dlor/dlor_script_$useDateStringNew.sql
   TXN_SCRIPTS_FILE_NAME=/data/store/view/loan_repayment_dashboard/scripts/txn/query/txn_script_$useDateStringNew.sql
   GET_TXN_SCRIPTS_FILE_NAME=/data/store/view/loan_repayment_dashboard/scripts/txn/get_txn/txn_script_$useDateStringNew.sql
   TXN_CLEAN_SCRIPT=/data/store/view/loan_repayment_dashboard/scripts/txn/clean_txn/clean_txn_script_$useDateStringNew.sql
   DLOR_HDFS_PATH=/data/loan_repayment/dlor/dlor_report_$useDateStringNew
   TXN_HDFS_PATH=/data/loan_repayment/txn/txn_mycredit_$useDateStringNew
   
   DLOR_OUTPUT_PATH=/data/store/view/loan_repayment_dashboard/raw_data/dlor/
   TXN_OUTPUT_PATH=/data/store/view/loan_repayment_dashboard/raw_data/txn/
   
   echo "$(date +%Y-%m-%d) Create SQL Query"
   
   echo "CREATE EXTERNAL TABLE loan_repayment_tracking.txn_mycredit_$useDateStringNew
   ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t'
   LINES TERMINATED BY '\n'
   STORED AS TEXTFILE AS
   SELECT txn.transaction_id, txn.transaction_customer_id, txn.transaction_amount, txn.transaction_datetime, txn.transaction_direction, txn.transaction_type, txn.destination_acct_product_type, txn.destination_acct_no, txn.destination_acct_type, txn.destination_acct_sub_type, txn.destination_acct_market_code, txn.destination_ending_balance, txn.loan_principal_amount, txn.loan_interest_amount, txn.loan_penalty_amount, txn.loan_fee_amount
   FROM prod_safe_timeline.eslip_pipeline txn
   WHERE (txn.destination_acct_type in ('7200')) AND (txn.destination_acct_sub_type in ('20011')) AND (txn.destination_acct_market_code in ('1370')) AND (txn.transaction_datetime >= '2022-11-01 00:00:00') AND (txn.daily_date >= '2022-11-01');" > $TXN_SCRIPTS_FILE_NAME
   
   echo "INSERT OVERWRITE DIRECTORY '$TXN_HDFS_PATH'
   ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t'
   LINES TERMINATED BY '\n'
   SELECT txn.*
   FROM loan_repayment_tracking.txn_mycredit_$useDateStringNew txn;" > $GET_TXN_SCRIPTS_FILE_NAME
   
   echo "INSERT OVERWRITE DIRECTORY '$DLOR_HDFS_PATH'
   ROW FORMAT DELIMITED
   FIELDS TERMINATED BY '\t'
   LINES TERMINATED BY '\n'
   SELECT dlor.*
   FROM prod_safe_credit_scoring.dlor_report dlor;" > $DLOR_SCRIPTS_FILE_NAME
   
   echo "DROP TABLE loan_repayment_tracking.txn_mycredit_$useDateStringNew;" > $TXN_CLEAN_SCRIPT
   
   echo "$(date +%Y-%m-%d) Create SQL Query Successfully"
   
   echo "$(date +%Y-%m-%d) Query Transactions and DLOR"
   
   hive -f $TXN_SCRIPTS_FILE_NAME &&
   hive -f $GET_TXN_SCRIPTS_FILE_NAME &&
   hdfs dfs -copyToLocal $TXN_HDFS_PATH $TXN_OUTPUT_PATH &&
   hive -f $DLOR_SCRIPTS_FILE_NAME &&
   hdfs dfs -copyToLocal $DLOR_HDFS_PATH $DLOR_OUTPUT_PATH &&
   hive -f $TXN_CLEAN_SCRIPT
   
   echo "$(date +%Y-%m-%d) Clean Temporary File"
   hdfs dfs -rm -r $DLOR_HDFS_PATH
   hdfs dfs -rm -r $TXN_HDFS_PATH
   
   echo "$(date +%Y-%m-%d) Query Transactions and DLOR Successfully"
   ```
   * Loan Repayment Simplified Shell Script (.sh)
   ```sh
   #!/bin/bash
   
   useDateString="$(date -d "$(date) - 1 day" "+%Y-%m-%d")"
   useDateStringNew="$(date -d "$(date) - 1 day" "+%Y%m%d")"
   outputPath=/data/store/view/loan_repayment_dashboard/loan_repayment_simplified/loan_repayment_simplified_${useDateStringNew}.tsv
   echo "gzip -c ${outputPath} > ${outputPath}.gz"
   bash /data/store/view/loan_repayment_dashboard/scripts/txn_dlor_mycredit_script.sh
   
   echo "$(date +%Y-%m-%d) Prepare Daily DLOR"
   docker run --rm -v /data/store/view/loan_repayment_dashboard/:/loan_repayment_dashboard datascience-loan-repayment-simplified-dlor-preparation:0.0.4 $useDateString /loan_repayment_dashboard/config/config_daily_dlor_docker
   
   echo "$(date +%Y-%m-%d) Prepare Loan Repayment Simplified"
   docker run --rm -v /data/store/view/loan_repayment_dashboard/:/loan_repayment_dashboard datascience-loan-repayment-simplified-calculation:0.0.4 $useDateString /loan_repayment_dashboard/config/config_loan_repayment_simplified_docker
   
   echo "$(date +%Y-%m-%d) Gzip Loan Repayment Simplified"
   gzip -c ${outputPath} > ${outputPath}.gz
   
   echo "$(date +%Y-%m-%d) Move Loan Repayment Simplified"
   mv ${outputPath}.gz /data/export_to_psql/loan_repayment_simp/
   
   chmod 644 /data/export_to_psql/loan_repayment_simp/loan_repayment_simplified_${useDateStringNew}.tsv.gz
   ```
### Run Command
- Program path: `/data/store/view/loan_repayment_dashboard`
- Git Repo: `https://github.com/mobilifeteam/datascience-loan-repayment-simplified.git`
- Daily DLOR Preparation Docker: `datascience-loan-repayment-simplified-dlor-preparation:0.0.4`
- Loan Repayment Simplified Docker: `datascience-loan-repayment-simplified-calculation:0.0.4`

Main Script Path: `/data/store/view/loan_repayment_dashboard/scripts/loan_repayment_simplified_automate.sh`
```sh
cd {Main Script Path}
nohup loan_repayment_simplified_automate.sh
```