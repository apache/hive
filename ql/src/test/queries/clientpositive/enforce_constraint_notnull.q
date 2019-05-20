--! qt:dataset:src
--  SIMPLE TABLE
-- create table with first and last column with not null
CREATE TABLE table1_n7 (a STRING NOT NULL ENFORCED, b STRING, c STRING NOT NULL ENFORCED);

-- insert value tuples
explain INSERT INTO table1_n7 values('not', 'null', 'constraint');
INSERT INTO table1_n7 values('not', 'null', 'constraint');
SELECT * FROM table1_n7;

-- insert with column specified
explain insert into table1_n7(a,c) values('1','2');
insert into table1_n7(a,c) values('1','2');

-- insert from select
explain INSERT INTO table1_n7 select key, src.value, value from src;
INSERT INTO table1_n7 select key, src.value, value from src;
SELECT * FROM table1_n7;

-- insert overwrite
explain INSERT OVERWRITE TABLE table1_n7 select src.*, value from src;
INSERT OVERWRITE TABLE table1_n7 select src.*, value from src;
SELECT * FROM table1_n7;

-- insert overwrite with if not exists
explain INSERT OVERWRITE TABLE table1_n7 if not exists select src.key, src.key, src.value from src;
INSERT OVERWRITE TABLE table1_n7 if not exists select src.key, src.key, src.value from src;
SELECT * FROM table1_n7;

DROP TABLE table1_n7;

-- multi insert
create table src_multi1_n0 (a STRING NOT NULL ENFORCED, b STRING);
create table src_multi2_n1 (i STRING, j STRING NOT NULL ENABLE);

explain
from src
insert overwrite table src_multi1_n0 select * where key < 10
insert overwrite table src_multi2_n1 select * where key > 10 and key < 20;


from src
insert overwrite table src_multi1_n0 select * where key < 10
insert overwrite table src_multi2_n1 select * where key > 10 and key < 20;

explain
from src
insert into table src_multi1_n0 select * where src.key < 10
insert into table src_multi2_n1 select src.* where key > 10 and key < 20;

from src
insert into table src_multi1_n0 select * where src.key < 10
insert into table src_multi2_n1 select src.* where key > 10 and key < 20;

--  ACID TABLE
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

-- SORT_QUERY_RESULTS
create table acid_uami_n1(i int,
                 de decimal(5,2) constraint nn1 not null enforced,
                 vc varchar(128) constraint nn2 not null enforced) clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

-- insert into values
explain insert into table acid_uami_n1 values
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');
insert into table acid_uami_n1 values
    (1, 109.23, 'mary had a little lamb'),
    (6553, 923.19, 'its fleece was white as snow');
select * from acid_uami_n1;

 --insert into select
explain insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src;
insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src;

-- select with limit
explain insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src limit 2;

-- select with order by
explain insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src
        order by key limit 2;

-- select with group by
explain insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src
        group by key, value order by key limit 2;

 --overwrite
explain insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src;
insert into table acid_uami_n1 select cast(key as int), cast (key as decimal(5,2)), value from src;

-- update
explain update acid_uami_n1 set de = 3.14 where de = 109.23 or de = 119.23;
update acid_uami_n1 set de = 3.14 where de = 109.23 or de = 119.23;

ALTER table acid_uami_n1 drop constraint nn1;
ALTER table acid_uami_n1 CHANGE i i int constraint nn0 not null enforced;

explain update acid_uami_n1 set de = 3.14159 where de = 3.14 ;
update acid_uami_n1 set de = 3.14159 where de = 3.14 ;

-- multi insert
explain
from src
insert overwrite table acid_uami_n1 select cast(key as int), cast(key as decimal(5,2)), value where key < 10
insert overwrite table src_multi2_n1 select * where key > 10 and key < 20;

set hive.exec.dynamic.partition.mode=nonstrict;
-- Table with partition
CREATE TABLE tablePartitioned (a STRING NOT NULL ENFORCED, b STRING, c STRING NOT NULL ENFORCED)
    PARTITIONED BY (p1 STRING, p2 INT NOT NULL DISABLE);

-- Insert into
explain INSERT INTO tablePartitioned partition(p1='today', p2=10) values('not', 'null', 'constraint');
INSERT INTO tablePartitioned partition(p1='today', p2=10) values('not', 'null', 'constraint');

-- Insert as select
explain INSERT INTO tablePartitioned partition(p1, p2) select key, value, value, key as p1, 3 as p2 from src limit 10;
INSERT INTO tablePartitioned partition(p1, p2) select key, value, value, key as p1, 3 as p2 from src limit 10;

select * from tablePartitioned;

-- multi insert
explain
from src
INSERT INTO tablePartitioned partition(p1, p2) select key, value, value, 'yesterday' as p1, 3 as p2
insert overwrite table src_multi2_n1 select * where key > 10 and key < 20;

DROP TABLE src_multi1_n0;
DROP TABLE src_multi2_n1;
DROP TABLE acid_uami_n1;

-- MERGE statements
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid_n2 (key int, a1 string, value string) stored as orc;

create table masking_test_n4 (key int NOT NULL enable, a1 string, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");

-- with cardinality check off
set hive.merge.cardinality.check=false;
explain MERGE INTO masking_test_n4 as t using nonacid_n2 as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

-- with cardinality check on
set hive.merge.cardinality.check=true;
explain MERGE INTO masking_test_n4 as t using nonacid_n2 as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

explain MERGE INTO masking_test_n4 as t using nonacid_n2 as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

explain MERGE INTO masking_test_n4 as t using nonacid_n2 as s ON t.key = s.key
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

-- shouldn't have constraint enforcement
explain MERGE INTO masking_test_n4 as t using nonacid_n2 as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE;

DROP TABLE masking_test_n4;
DROP TABLE nonacid_n2;

-- Test drop constraint
create table table2_n3(i int constraint nn5 not null enforced, j int);
explain insert into table2_n3 values(2, 3);
alter table table2_n3 drop constraint nn5;
explain insert into table2_n3 values(2, 3);
DROP TABLE table2_n3;

-- temporary table
create temporary table tttemp(i int not null enforced);
explain insert into tttemp values(1);
explain insert into tttemp select cast(key as int) from src;
drop table tttemp;

-- micro-managed table
set hive.create.as.insert.only=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
create table part_mm_n1(key int not null enforced) partitioned by (key_mm int) stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
explain insert into table part_mm_n1 partition(key_mm=455) select key from src order by value limit 3;
insert into table part_mm_n1 partition(key_mm=455) select key from src order by value limit 3;
select key from src order by value limit 3;
select * from part_mm_n1;
drop table part_mm_n1;

