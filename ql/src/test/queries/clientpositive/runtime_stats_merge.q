
set hive.mapred.mode=nonstrict;
set hive.security.authorization.manager=org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest;
set hive.support.concurrency=true;
set hive.explain.user=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=88888888;
-- set hive.auto.convert.sortmerge.join=true;
-- set hive.auto.convert.sortmerge.join.to.mapjoin=true;

create table lineitem1 (L_ORDERKEY integer);

insert into lineitem1 values (1),(2),(3);

create table lineitem2
 stored as orc  TBLPROPERTIES ('transactional'='true')
  as select * from lineitem1;
create table lineitem_stage
 stored as orc  TBLPROPERTIES ('transactional'='true')
  as select * from lineitem1 limit 1;


analyze table lineitem2 compute statistics for columns;
analyze table lineitem_stage compute statistics for columns;

explain reoptimization
merge into lineitem2 using
	(select * from lineitem_stage) sub
	on sub.L_ORDERKEY = lineitem2.L_ORDERKEY
	when matched then delete;

merge into lineitem2 using
	(select * from lineitem_stage) sub
	on sub.L_ORDERKEY = lineitem2.L_ORDERKEY
	when matched then delete;

	
