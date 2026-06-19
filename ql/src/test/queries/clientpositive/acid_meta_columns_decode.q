set hive.mapred.mode=nonstrict;
set hive.fetch.task.conversion=none;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


CREATE transactional TABLE acidTblDefault(a int, b INT) STORED AS ORC;
INSERT INTO TABLE acidTblDefault VALUES (1,2),(2,3),(3,4);

set hive.optimize.acid.meta.columns=true;

select a,b from acidTblDefault;
select b from acidTblDefault;
select ROW__ID, b from acidTblDefault;
select a, ROW__ID, b from acidTblDefault;
select a, ROW__ID from acidTblDefault;

set hive.optimize.acid.meta.columns=false;

select a,b from acidTblDefault;
select b from acidTblDefault;
select ROW__ID, b from acidTblDefault;
select a, ROW__ID, b from acidTblDefault;
select a, ROW__ID from acidTblDefault;
