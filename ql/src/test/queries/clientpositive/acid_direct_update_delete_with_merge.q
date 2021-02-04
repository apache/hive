-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.acid.direct.insert.enabled=true;

DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS merge_source;

CREATE TABLE transactions (id int, value string, last_update_user string) PARTITIONED BY (tran_date string) CLUSTERED BY (id) into 5 buckets  STORED AS ORC TBLPROPERTIES ('transactional'='true');
CREATE TABLE merge_source (id int, value string, tran_date string) STORED AS ORC;

INSERT INTO transactions PARTITION (tran_date) VALUES
(1, 'value_01', 'creation', '20170410'),
(2, 'value_02', 'creation', '20170410'),
(3, 'value_03', 'creation', '20170410'),
(4, 'value_04', 'creation', '20170410'),
(5, 'value_05', 'creation', '20170413'),
(6, 'value_06', 'creation', '20170413'),
(7, 'value_07', 'creation', '20170413'),
(8, 'value_08', 'creation', '20170413'),
(9, 'value_09', 'creation', '20170413'),
(10, 'value_10','creation', '20170413');

INSERT INTO merge_source VALUES
(1, 'value_01', '20170410'),
(4, NULL, '20170410'),
(7, 'value_77777', '20170413'),
(8, NULL, '20170413'),
(8, 'value_08', '20170415'),
(11, 'value_11', '20170415');

MERGE INTO transactions AS T
USING merge_source AS S
ON T.ID = S.ID and T.tran_date = S.tran_date
WHEN MATCHED AND (T.value != S.value AND S.value IS NOT NULL) THEN UPDATE SET value = S.value, last_update_user = 'merge_update'
WHEN MATCHED AND S.value IS NULL THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (S.ID, S.value, 'merge_insert', S.tran_date);

SELECT * FROM transactions;

DROP TABLE IF EXISTS transactions;
DROP TABLE IF EXISTS merge_source;