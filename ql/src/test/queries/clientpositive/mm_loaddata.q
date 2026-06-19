set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set mapreduce.map.memory.mb=128;


drop table load0_mm;
create table load0_mm (key string, value string) stored as textfile tblproperties("transactional"="true", "transactional_properties"="insert_only");
load data local inpath '../../data/files/kv1.txt' into table load0_mm;
select count(1) from load0_mm;
load data local inpath '../../data/files/kv2.txt' into table load0_mm;
select count(1) from load0_mm;
load data local inpath '../../data/files/kv2.txt' overwrite into table load0_mm;
select count(1) from load0_mm;
drop table load0_mm;


drop table intermediate2;
create table intermediate2 (key string, value string) stored as textfile
location 'file:${system:test.tmp.dir}/intermediate2';
load data local inpath '../../data/files/kv1.txt' into table intermediate2;
load data local inpath '../../data/files/kv2.txt' into table intermediate2;
load data local inpath '../../data/files/kv3.txt' into table intermediate2;

drop table load1_mm;
create table load1_mm (key string, value string) stored as textfile tblproperties("transactional"="true", "transactional_properties"="insert_only");
load data inpath 'file:${system:test.tmp.dir}/intermediate2/kv2.txt' into table load1_mm;
load data inpath 'file:${system:test.tmp.dir}/intermediate2/kv1.txt' into table load1_mm;
select count(1) from load1_mm;
load data local inpath '../../data/files/kv1.txt' into table intermediate2;
load data local inpath '../../data/files/kv2.txt' into table intermediate2;
load data local inpath '../../data/files/kv3.txt' into table intermediate2;
load data inpath 'file:${system:test.tmp.dir}/intermediate2/kv*.txt' overwrite into table load1_mm;
select count(1) from load1_mm;
load data local inpath '../../data/files/kv2.txt' into table intermediate2;
load data inpath 'file:${system:test.tmp.dir}/intermediate2/kv2.txt' overwrite into table load1_mm;
select count(1) from load1_mm;
drop table load1_mm;

drop table load2_mm;
create table load2_mm (key string, value string)
  partitioned by (k int, l int) stored as textfile tblproperties("transactional"="true", "transactional_properties"="insert_only");
load data local inpath '../../data/files/kv1.txt' into table intermediate2;
load data local inpath '../../data/files/kv2.txt' into table intermediate2;
load data local inpath '../../data/files/kv3.txt' into table intermediate2;
load data inpath 'file:${system:test.tmp.dir}/intermediate2/kv*.txt' into table load2_mm partition(k=5, l=5);
select count(1) from load2_mm;
drop table load2_mm;
drop table intermediate2;