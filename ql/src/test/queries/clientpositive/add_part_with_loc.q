set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.dynamic.partition=true;
set hive.vectorized.execution.enabled=true;


set hive.fileformat.check=true;

create table supply (id int, part string, quantity int) partitioned by (day int)
	stored as orc
	location 'hdfs:///tmp/a1'
	TBLPROPERTIES ('transactional'='true')
;


explain alter table supply add partition (day=20110102) location 
	'hdfs:///tmp/a2';

alter table supply add partition (day=20110103) location 
	'hdfs:///tmp/a3';


