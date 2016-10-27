set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.tez.auto.reducer.parallelism=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 2;


drop table multi0_1_mm;
drop table multi0_2_mm;
create table multi0_1_mm (key int, key2 int)  tblproperties("transactional"="true", "transactional_properties"="insert_only");
create table multi0_2_mm (key int, key2 int)  tblproperties("transactional"="true", "transactional_properties"="insert_only");

from intermediate
insert overwrite table multi0_1_mm select key, p
insert overwrite table multi0_2_mm select p, key;

select * from multi0_1_mm order by key, key2;
select * from multi0_2_mm order by key, key2;

set hive.merge.mapredfiles=true;
set hive.merge.sparkfiles=true;
set hive.merge.tezfiles=true;

from intermediate
insert into table multi0_1_mm select p, key
insert overwrite table multi0_2_mm select key, p;
select * from multi0_1_mm order by key, key2;
select * from multi0_2_mm order by key, key2;

set hive.merge.mapredfiles=false;
set hive.merge.sparkfiles=false;
set hive.merge.tezfiles=false;

drop table multi0_1_mm;
drop table multi0_2_mm;


drop table multi1_mm;
create table multi1_mm (key int, key2 int) partitioned by (p int) tblproperties("transactional"="true", "transactional_properties"="insert_only");
from intermediate
insert into table multi1_mm partition(p=1) select p, key
insert into table multi1_mm partition(p=2) select key, p;
select * from multi1_mm order by key, key2, p;
from intermediate
insert into table multi1_mm partition(p=2) select p, key
insert overwrite table multi1_mm partition(p=1) select key, p;
select * from multi1_mm order by key, key2, p;

from intermediate
insert into table multi1_mm partition(p) select p, key, p
insert into table multi1_mm partition(p=1) select key, p;
select key, key2, p from multi1_mm order by key, key2, p;

from intermediate
insert into table multi1_mm partition(p) select p, key, 1
insert into table multi1_mm partition(p=1) select key, p;
select key, key2, p from multi1_mm order by key, key2, p;
drop table multi1_mm;



drop table intermediate;


