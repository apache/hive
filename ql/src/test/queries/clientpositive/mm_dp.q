--! qt:dataset:src1
--! qt:dataset:src

-- MASK_LINEAGE

set hive.metastore.dml.events=true;
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set mapred.max.split.size=5000;
set mapred.reduce.tasks=10;
set tez.am.grouping.split-count=10;
set tez.grouping.split-count=10;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.exec.max.dynamic.partitions=1000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.optimize.sort.dynamic.partition=true;

-- Force multiple writers when reading
drop table dp_mm;
drop table intermediate_n0;

create table intermediate_n0(key int, val string, r int) partitioned by (p int) stored as orc tblproperties("transactional"="false");
insert into table intermediate_n0 partition(p='455') select *, cast(rand(12345) * 30 as int) from src where key < 200;
insert into table intermediate_n0 partition(p='456') select *, cast(rand(12345) * 30 as int) from src where key >= 200;
insert into table intermediate_n0 partition(p='457') select *, cast(rand(12345) * 30 as int) from src where key < 200;
insert into table intermediate_n0 partition(p='457') select *, cast(rand(12345) * 30 as int) from src;
insert into table intermediate_n0 partition(p='458') select *, cast(rand(12345) * 30 as int) from src;
insert into table intermediate_n0 partition(p='458') select *, cast(rand(12345) * 30 as int) from src where key >= 100;
insert into table intermediate_n0 partition(p='459') select *, cast(rand(12345) * 30 as int) from src;


CREATE TABLE dp_mm(key int, p int, r int) PARTITIONED BY (val string)
CLUSTERED BY (r) SORTED BY (r) INTO 3 BUCKETS
STORED AS ORC  tblproperties("transactional"="true", "transactional_properties"="insert_only");

explain 
insert overwrite table dp_mm partition (val) select key, p, r, val from intermediate_n0;

insert overwrite table dp_mm partition (val) select key, p, r, val from intermediate_n0;

select * from dp_mm order by key, p, r, val;

drop table dp_mm;
drop table intermediate_n0;


