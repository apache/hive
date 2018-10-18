--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


-- Bucketing tests are slow and some tablesample ones don't work w/o MM

-- Force multiple writers when reading
drop table intermediate_n2;
create table intermediate_n2(key int) partitioned by (p int) stored as orc;
insert into table intermediate_n2 partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate_n2 partition(p='456') select distinct key from src where key is not null order by key asc limit 2;
insert into table intermediate_n2 partition(p='457') select distinct key from src where key >= 100 order by key asc limit 2;



drop table bucket0_mm;
create table bucket0_mm(key int, id int)
clustered by (key) into 2 buckets
tblproperties("transactional"="true", "transactional_properties"="insert_only");
insert into table bucket0_mm select key, key from intermediate_n2;
select * from bucket0_mm order by key, id;
select * from bucket0_mm tablesample (bucket 1 out of 2) s;
select * from bucket0_mm tablesample (bucket 2 out of 2) s;
insert into table bucket0_mm select key, key from intermediate_n2;
select * from bucket0_mm order by key, id;
select * from bucket0_mm tablesample (bucket 1 out of 2) s;
select * from bucket0_mm tablesample (bucket 2 out of 2) s;
drop table bucket0_mm;


drop table bucket1_mm;
create table bucket1_mm(key int, id int) partitioned by (key2 int)
clustered by (key) sorted by (key) into 2 buckets
tblproperties("transactional"="true", "transactional_properties"="insert_only");
insert into table bucket1_mm partition (key2)
select key + 1, key, key - 1 from intermediate_n2
union all 
select key - 1, key, key + 1 from intermediate_n2;
select * from bucket1_mm order by key, id;
select * from bucket1_mm tablesample (bucket 1 out of 2) s  order by key, id;
select * from bucket1_mm tablesample (bucket 2 out of 2) s  order by key, id;
drop table bucket1_mm;



drop table bucket2_mm;
create table bucket2_mm(key int, id int)
clustered by (key) into 10 buckets
tblproperties("transactional"="true", "transactional_properties"="insert_only");
insert into table bucket2_mm select key, key from intermediate_n2 where key == 0;
select * from bucket2_mm order by key, id;
select * from bucket2_mm tablesample (bucket 1 out of 10) s order by key, id;
select * from bucket2_mm tablesample (bucket 4 out of 10) s order by key, id;
insert into table bucket2_mm select key, key from intermediate_n2 where key in (0, 103);
select * from bucket2_mm;
select * from bucket2_mm tablesample (bucket 1 out of 10) s order by key, id;
select * from bucket2_mm tablesample (bucket 4 out of 10) s order by key, id;
drop table bucket2_mm;

drop table intermediate_n2;