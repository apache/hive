-- SORT_QUERY_RESULTS

set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.stats.autogather=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.dynamic.partition=true;

DROP TABLE txt_table;
DROP TABLE part_table;

CREATE TABLE txt_table (id int, value string, year string, month string, day string);
INSERT INTO TABLE txt_table VALUES (1, 'one', '2020', '02', '11');
INSERT INTO TABLE txt_table VALUES (2, 'two', '2019', '03', '12');
INSERT INTO TABLE txt_table VALUES (3, 'three', '2020', '03', '13');

CREATE TABLE part_table (id int, value string) PARTITIONED BY(year string, month string, day string) STORED AS ORC TBLPROPERTIES('transactional'='true','transactional_properties'='insert_only');

INSERT INTO TABLE part_table PARTITION (year, month, day) SELECT id, value, year, month, day FROM txt_table ;
SELECT * FROM part_table;

DROP TABLE txt_table;
DROP TABLE part_table;
