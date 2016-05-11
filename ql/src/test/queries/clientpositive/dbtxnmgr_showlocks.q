set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

show locks;

show locks extended;

show locks default;

show transactions;

create table partitioned_acid_table (a int, b int) partitioned by (p string) clustered by (a) into 2 buckets stored as orc tblproperties ('transactional'='true');

show locks database default;

show locks partitioned_acid_table;

show locks partitioned_acid_table extended;

show locks partitioned_acid_table partition (p='abc');

show locks partitioned_acid_table partition (p='abc') extended;

drop table partitioned_acid_table;
