
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 2;
insert into table intermediate partition(p='457') select distinct key from src where key >= 100 order by key asc limit 2;

drop table cttas1_mm;

create temporary table cttas1_mm tblproperties ("transactional"="true", "transactional_properties"="insert_only") as select * from intermediate;

select * from cttas1_mm;

drop table cttas1_mm;

drop table intermediate;

