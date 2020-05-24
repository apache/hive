set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set mapreduce.map.memory.mb=1024;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.vectorized.execution.enabled=true;
set hive.vectorized.use.vector.serde.deserialize=true;
set hive.vectorized.use.row.serde.deserialize=true;

drop table load0_ss;
create table load0_ss (key string, value string) stored as textfile tblproperties("transactional"="true", "transactional_properties"="insert_only");

load data local inpath '../../data/files/kv1.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv2.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv1.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv2.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv1.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv2.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv1.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv2.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv1.txt' into table load0_ss;
select count(1) from load0_ss;
load data local inpath '../../data/files/kv2.txt' into table load0_ss;
select count(1) from load0_ss;

drop table load0_ss;