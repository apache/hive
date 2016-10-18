set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


drop table qtr_acid;
create table qtr_acid (key int) partitioned by (p int) tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table qtr_acid partition(p='123') select distinct key from src where key > 0 order by key asc limit 10;
insert into table qtr_acid partition(p='456') select distinct key from src where key > 0 order by key desc limit 10;
explain
select * from qtr_acid order by key;
select * from qtr_acid order by key;
drop table qtr_acid;