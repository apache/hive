--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- Force multiple writers when reading
drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 1;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 1;
insert into table intermediate partition(p='457') select distinct key from src where key >= 100 order by key asc limit 1;

set hive.mm.allow.originals=true;
set hive.exim.test.mode=true;

drop table simple_to_mm;
create table simple_to_mm(key int) stored as orc tblproperties("transactional"="false");
insert into table simple_to_mm select key from intermediate;
select * from simple_to_mm s1 order by key;

explain alter table simple_to_mm convert to acid tblproperties ("transactional_properties"="insert_only");
alter table simple_to_mm convert to acid tblproperties ("transactional_properties"="insert_only");
export table simple_to_mm to 'ql/test/data/exports/export0';
select * from simple_to_mm s2 order by key;
create table import_converted0_mm(key int) stored as orc tblproperties("transactional"="false");
import table import_converted0_mm from 'ql/test/data/exports/export0';
select * from import_converted0_mm order by key;
drop table import_converted0_mm;

insert into table simple_to_mm select key from intermediate;
insert into table simple_to_mm select key from intermediate;
export table simple_to_mm to 'ql/test/data/exports/export1';
select * from simple_to_mm s3 order by key;
create table import_converted1_mm(key int) stored as orc tblproperties("transactional"="false");
import table import_converted1_mm from 'ql/test/data/exports/export1';
select * from import_converted1_mm order by key;
drop table import_converted1_mm;

insert overwrite table simple_to_mm select key from intermediate;
export table simple_to_mm to 'ql/test/data/exports/export2';
select * from simple_to_mm s4 order by key;
create table import_converted2_mm(key int) stored as orc tblproperties("transactional"="false");
import table import_converted2_mm from 'ql/test/data/exports/export2';
select * from import_converted2_mm order by key;
drop table import_converted2_mm;
drop table simple_to_mm;


drop table part_to_mm;
create table part_to_mm(key int) partitioned by (key_mm int) stored as orc tblproperties("transactional"="false");
insert into table part_to_mm partition(key_mm='455') select key from intermediate;
insert into table part_to_mm partition(key_mm='456') select key from intermediate;
select * from part_to_mm s1 order by key, key_mm;
alter table part_to_mm set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select * from part_to_mm s2 order by key, key_mm;
insert into table part_to_mm partition(key_mm='456') select key from intermediate;
insert into table part_to_mm partition(key_mm='457') select key from intermediate;
select * from part_to_mm s3 order by key, key_mm;
drop table part_to_mm;


drop table load_to_mm;
create table load_to_mm (key string, value string) tblproperties("transactional"="false");
load data local inpath '../../data/files/kv1.txt' into table load_to_mm;
load data local inpath '../../data/files/kv1.txt' into table load_to_mm;
select count(*) from load_to_mm s1;
alter table load_to_mm set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select count(*) from load_to_mm s2;
drop table load_to_mm;


set hive.mm.allow.originals=false;

drop table simple_to_mm_text;
create table simple_to_mm_text(key int) stored as textfile tblproperties("transactional"="false");
insert into table simple_to_mm_text select key from intermediate;
select * from simple_to_mm_text t1 order by key;
alter table simple_to_mm_text set tblproperties("transactional"="true", "transactional_properties"="insert_only");
select * from simple_to_mm_text t2 order by key;
insert into table simple_to_mm_text select key from intermediate;
insert into table simple_to_mm_text select key from intermediate;
select * from simple_to_mm_text t3 order by key;
insert overwrite table simple_to_mm_text select key from intermediate;
select * from simple_to_mm_text t4 order by key;
drop table simple_to_mm_text;

drop table intermediate;
