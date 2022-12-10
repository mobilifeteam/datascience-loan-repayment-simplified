#!/bin/bash

useDateString="$(date -d "$(date) - 1 day" "+%Y-%m-%d")"
useDateStringNew="$(date -d "$(date) - 1 day" "+%Y%m%d")"

DLOR_SCRIPTS_FILE_NAME=/data/store/view/loan_repayment_dashboard/scripts/dlor/get_dlor/dlor_script_$useDateStringNew.sql
TXN_SCRIPTS_FILE_NAME=/data/store/view/loan_repayment_dashboard/scripts/txn/query/txn_script_$useDateStringNew.sql
GET_TXN_SCRIPTS_FILE_NAME=/data/store/view/loan_repayment_dashboard/scripts/txn/get_txn/txn_script_$useDateStringNew.sql

TXN_CLEAN_SCRIPT=/data/store/view/loan_repayment_dashboard/scripts/txn/clean_txn/clean_txn_script_$useDateStringNew.sql

DLOR_HDFS_PATH=/data/loan_repayment/dlor/dlor_report_$useDateStringNew
TXN_HDFS_PATH=/data/loan_repayment/txn/txn_mycredit_$useDateStringNew

DLOR_OUTPUT_PATH=/data/store/view/loan_repayment_dashboard/raw_data/dlor/dlor_report_$useDateStringNew
TXN_OUTPUT_PATH=/data/store/view/loan_repayment_dashboard/raw_data/txn/txn_mycredit_$useDateStringNew

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
hdfs dfs -rm -r -f $DLOR_HDFS_PATH
hdfs dfs -rm -r -f $TXN_HDFS_PATH 

echo "$(date +%Y-%m-%d) Query Transactions and DLOR Successfully"
