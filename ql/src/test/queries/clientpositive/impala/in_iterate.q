--! qt:dataset:impala_dataset

EXPLAIN CBO
SELECT
  ss_sold_time_sk
FROM
  impala_hive_tpcds_store_sales
WHERE 
  ss_cdemo_sk in (ss_hdemo_sk)
LIMIT 50;
