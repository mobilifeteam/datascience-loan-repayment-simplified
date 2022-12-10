#!/bin/bash

useDateString="$(date -d "$(date) - 1 day" "+%Y-%m-%d")"
useDateStringNew="$(date -d "$(date) - 1 day" "+%Y%m%d")"
outputPath=/data/store/view/loan_repayment_dashboard/loan_repayment_simplified/loan_repayment_simplified_${useDateStringNew}.tsv
echo "gzip -c ${outputPath} > ${outputPath}.gz"
bash /data/store/view/loan_repayment_dashboard/scripts/txn_dlor_mycredit_script.sh

echo "$(date +%Y-%m-%d) Prepare Daily DLOR"
docker run --rm -v /data/store/view/loan_repayment_dashboard/:/loan_repayment_dashboard datascience-loan-repayment-simplified-dlor-preparation:0.0.1 $useDateString /loan_repayment_dashboard/config/config_daily_dlor_docker

echo "$(date +%Y-%m-%d) Prepare Loan Repayment Simplified"
docker run --rm -v /data/store/view/loan_repayment_dashboard/:/loan_repayment_dashboard datascience-loan-repayment-simplified-calculation:0.0.1 $useDateString /loan_repayment_dashboard/config/config_loan_repayment_simplified_docker

echo "$(date +%Y-%m-%d) Gzip Loan Repayment Simplified"
gzip -c ${outputPath} > ${outputPath}.gz

echo "$(date +%Y-%m-%d) Move Loan Repayment Simplified"
mv ${outputPath}.gz /data/export_to_psql/loan_repayment_simp/

chmod 644 /data/export_to_psql/loan_repayment_simp/loan_repayment_simplified_${useDateStringNew}.tsv.gz





