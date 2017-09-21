set hive.mapred.mode=nonstrict;
set hive.explain.user=false;
set hive.fetch.task.conversion=none;
set tez.grouping.min-size=1;
set tez.grouping.max-size=2;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;


-- Force multiple writers when reading
drop table intermediate;
create table intermediate(key int) partitioned by (p int) stored as orc;
insert into table intermediate partition(p='455') select distinct key from src where key >= 0 order by key desc limit 2;
insert into table intermediate partition(p='456') select distinct key from src where key is not null order by key asc limit 2;
insert into table intermediate partition(p='457') select distinct key from src where key >= 100 order by key asc limit 2;


drop table part_mm;
create table part_mm(key int) partitioned by (key_mm int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
explain insert into table part_mm partition(key_mm=455) select key from intermediate;
insert into table part_mm partition(key_mm=455) select key from intermediate;
insert into table part_mm partition(key_mm=456) select key from intermediate;
insert into table part_mm partition(key_mm=455) select key from intermediate;
select * from part_mm order by key, key_mm;

-- TODO: doesn't work truncate table part_mm partition(key_mm=455);
select * from part_mm order by key, key_mm;
truncate table part_mm;
select * from part_mm order by key, key_mm;
drop table part_mm;

drop table simple_mm;
create table simple_mm(key int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table simple_mm select key from intermediate;
select * from simple_mm order by key;
insert into table simple_mm select key from intermediate;
select * from simple_mm order by key;
truncate table simple_mm;
select * from simple_mm;
drop table simple_mm;


-- simple DP (no bucketing)
drop table dp_mm;

set hive.exec.dynamic.partition.mode=nonstrict;

set hive.merge.mapredfiles=false;
set hive.merge.sparkfiles=false;
set hive.merge.tezfiles=false;

create table dp_mm (key int) partitioned by (key1 string, key2 int) stored as orc
  tblproperties ("transactional"="true", "transactional_properties"="insert_only");

insert into table dp_mm partition (key1='123', key2) select key, key from intermediate;

select * from dp_mm order by key;

drop table dp_mm;


-- union

create table union_mm(id int)  tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table union_mm 
select temps.p from (
select key as p from intermediate 
union all 
select key + 1 as p from intermediate ) temps;

select * from union_mm order by id;

insert into table union_mm 
select p from
(
select key + 1 as p from intermediate
union all
select key from intermediate
) tab group by p
union all
select key + 2 as p from intermediate;

select * from union_mm order by id;

insert into table union_mm
SELECT p FROM
(
  SELECT key + 1 as p FROM intermediate
  UNION ALL
  SELECT key as p FROM ( 
    SELECT distinct key FROM (
      SELECT key FROM (
        SELECT key + 2 as key FROM intermediate
        UNION ALL
        SELECT key FROM intermediate
      )t1 
    group by key)t2
  )t3
)t4
group by p;


select * from union_mm order by id;
drop table union_mm;


create table partunion_mm(id int) partitioned by (key int) tblproperties ("transactional"="true", "transactional_properties"="insert_only");
insert into table partunion_mm partition(key)
select temps.* from (
select key as p, key from intermediate 
union all 
select key + 1 as p, key + 1 from intermediate ) temps;

select * from partunion_mm order by id;
drop table partunion_mm;



create table skew_mm(k1 int, k2 int, k4 int) skewed by (k1, k4) on ((0,0),(1,1),(2,2),(3,3))
 stored as directories tblproperties ("transactional"="true", "transactional_properties"="insert_only");

insert into table skew_mm 
select key, key, key from intermediate;

select * from skew_mm order by k2, k1, k4;
drop table skew_mm;


create table skew_dp_union_mm(k1 int, k2 int, k4 int) partitioned by (k3 int) 
skewed by (k1, k4) on ((0,0),(1,1),(2,2),(3,3)) stored as directories tblproperties ("transactional"="true", "transactional_properties"="insert_only");

insert into table skew_dp_union_mm partition (k3)
select key as i, key as j, key as k, key as l from intermediate
union all 
select key +1 as i, key +2 as j, key +3 as k, key +4 as l from intermediate;


select * from skew_dp_union_mm order by k2, k1, k4;
drop table skew_dp_union_mm;



set hive.merge.orcfile.stripe.level=true;
set hive.merge.tezfiles=true;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;


create table merge0_mm (id int) stored as orc tblproperties("transactional"="true", "transactional_properties"="insert_only");

insert into table merge0_mm select key from intermediate;
select * from merge0_mm;

set tez.grouping.split-count=1;
insert into table merge0_mm select key from intermediate;
set tez.grouping.split-count=0;
select * from merge0_mm;

drop table merge0_mm;


create table merge2_mm (id int) tblproperties("transactional"="true", "transactional_properties"="insert_only");

insert into table merge2_mm select key from intermediate;
select * from merge2_mm;

set tez.grouping.split-count=1;
insert into table merge2_mm select key from intermediate;
set tez.grouping.split-count=0;
select * from merge2_mm;

drop table merge2_mm;


create table merge1_mm (id int) partitioned by (key int) stored as orc tblproperties("transactional"="true", "transactional_properties"="insert_only");

insert into table merge1_mm partition (key) select key, key from intermediate;
select * from merge1_mm order by id, key;

set tez.grouping.split-count=1;
insert into table merge1_mm partition (key) select key, key from intermediate;
set tez.grouping.split-count=0;
select * from merge1_mm order by id, key;

drop table merge1_mm;

set hive.merge.tezfiles=false;
set hive.merge.mapfiles=false;
set hive.merge.mapredfiles=false;

-- TODO: need to include merge+union+DP, but it's broken for now


drop table ctas0_mm;
create table ctas0_mm tblproperties ("transactional"="true", "transactional_properties"="insert_only") as select * from intermediate;
select * from ctas0_mm;
drop table ctas0_mm;

drop table ctas1_mm;
create table ctas1_mm tblproperties ("transactional"="true", "transactional_properties"="insert_only") as
  select * from intermediate union all select * from intermediate;
select * from ctas1_mm;
drop table ctas1_mm;


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




set datanucleus.cache.collections=false;
set hive.stats.autogather=true;

drop table stats_mm;
create table stats_mm(key int)  tblproperties("transactional"="true", "transactional_properties"="insert_only");
--insert overwrite table stats_mm  select key from intermediate;
insert into table stats_mm  select key from intermediate;
desc formatted stats_mm;

insert into table stats_mm  select key from intermediate;
desc formatted stats_mm;
drop table stats_mm;

drop table stats2_mm;
create table stats2_mm tblproperties("transactional"="true", "transactional_properties"="insert_only") as select array(key, value) from src;
desc formatted stats2_mm;
drop table stats2_mm;


set hive.optimize.skewjoin=true;
set hive.skewjoin.key=2;
set hive.optimize.metadataonly=false;

CREATE TABLE skewjoin_mm(key INT, value STRING) STORED AS TEXTFILE tblproperties ("transactional"="true", "transactional_properties"="insert_only");
FROM src src1 JOIN src src2 ON (src1.key = src2.key) INSERT into TABLE skewjoin_mm SELECT src1.key, src2.value;
select count(distinct key) from skewjoin_mm;
drop table skewjoin_mm;

set hive.optimize.skewjoin=false;

set hive.optimize.index.filter=true;
set hive.auto.convert.join=false;
CREATE TABLE parquet1_mm(id INT) STORED AS PARQUET tblproperties ("transactional"="true", "transactional_properties"="insert_only");
INSERT INTO parquet1_mm VALUES(1), (2);
CREATE TABLE parquet2_mm(id INT, value STRING) STORED AS PARQUET tblproperties ("transactional"="true", "transactional_properties"="insert_only");
INSERT INTO parquet2_mm VALUES(1, 'value1');
INSERT INTO parquet2_mm VALUES(1, 'value2');
select parquet1_mm.id, t1.value, t2.value FROM parquet1_mm
  JOIN parquet2_mm t1 ON parquet1_mm.id=t1.id
  JOIN parquet2_mm t2 ON parquet1_mm.id=t2.id
where t1.value = 'value1' and t2.value = 'value2';
drop table parquet1_mm;
drop table parquet2_mm;

set hive.auto.convert.join=true;


DROP TABLE IF EXISTS temp1;
CREATE TEMPORARY TABLE temp1 (a int) TBLPROPERTIES ("transactional"="true", "transactional_properties"="insert_only");
INSERT INTO temp1 SELECT key FROM intermediate;
DESC EXTENDED temp1;
SELECT * FROM temp1;


drop table intermediate;



