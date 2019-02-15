set hive.stats.autogather=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

CREATE TABLE table1_n0(i int CHECK (-i > -10),
    j int CHECK (+j > 10),
    ij boolean CHECK (ij IS NOT NULL),
    a int CHECK (a BETWEEN i AND j),
    bb float CHECK (bb IN (23.4,56,4)),
    d bigint CHECK (d > round(567.6) AND d < round(1000.4)))
    clustered by (i) into 2 buckets stored as orc TBLPROPERTIES('transactional'='true');
DESC FORMATTED table1_n0;

EXPLAIN INSERT INTO table1_n0 values(1,100,true, 5, 23.4, 700.5);
INSERT INTO table1_n0 values(1,100,true, 5, 23.4, 700.5);
SELECT * from table1_n0;
DROP TABLE table1_n0;

-- null check constraint
CREATE TABLE table2_n0(i int CHECK (i + NULL > 0));
DESC FORMATTED table2_n0;
EXPLAIN INSERT INTO table2_n0 values(8);
INSERT INTO table2_n0 values(8);
select * from table2_n0;
Drop table table2_n0;

-- UDF created by users
CREATE FUNCTION test_udf2 AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDFTestGetJavaString';
CREATE TABLE tudf(v string CHECK (test_udf2(v) <> 'vin'));
EXPLAIN INSERT INTO tudf values('function1');
Drop table tudf;

-- multiple constraints
create table tmulti(url string NOT NULL ENABLE, userName string, numClicks int CHECK (numClicks > 0), d date);
alter table tmulti add constraint un1 UNIQUE (userName, numClicks) DISABLE;
DESC formatted tmulti;
EXPLAIN INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
Select * from tmulti;

-- alter table add constraint
truncate table tmulti;
alter table tmulti add constraint chk1 CHECK (userName != NULL);
alter table tmulti add constraint chk2 CHECK (numClicks <= 10000 AND userName != '');
DESC formatted tmulti;
EXPLAIN INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
INSERT INTO tmulti values('hive.apache.com', 'user1', 48, '12-01-2018');
Select * from tmulti;
Drop table tmulti;

-- case insentivity
create table tcase(url string NOT NULL ENABLE, userName string, d date, numClicks int CHECK (numclicks > 0));
DESC formatted tcase;
EXPLAIN INSERT INTO tcase values('hive.apache.com', 'user1', '12-01-2018', 48);
INSERT INTO tcase values('hive.apache.com', 'user1', '12-01-2018', 48);
Select * from tcase ;
Drop table tcase;

-- cast
create table tcast(url string NOT NULL ENABLE, numClicks int,
    price FLOAT CHECK (cast(numClicks as FLOAT)*price > 10.00));
DESC FORMATTED tcast;
EXPLAIN INSERT INTO tcast values('www.google.com', 100, cast(0.5 as float));
INSERT INTO tcast values('www.google.com', 100, cast(0.5 as float));
SELECT * from tcast;
-- check shouldn't fail
EXPLAIN INSERT INTO tcast(url, price) values('www.yahoo.com', 0.5);
INSERT INTO tcast(url, price) values('www.yahoo.com', 0.5);
SELECT * FROM tcast;
DROP TABLE tcast;

-- complex expression
create table texpr(i int DEFAULT 89, f float NOT NULL ENABLE, d decimal(4,1),
    b boolean CHECK (((cast(d as float) + f) < cast(i as float) + (i*i))));
DESC FORMATTED texpr;
explain insert into texpr values(3,3.4,5.6,true);
insert into texpr values(3,3.4,5.6,true);
SELECT * from texpr;
DROP TABLE texpr;

-- UPDATE
create table acid_uami_n0(i int,
                 de decimal(5,2) constraint nn1 not null enforced,
                 vc varchar(128) constraint ch2 CHECK (de >= cast(i as decimal(5,2))) enforced)
                 clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED acid_uami_n0;

--! qt:dataset:src
-- insert as select
explain insert into table acid_uami_n0 select cast(key as int), cast (key as decimal(5,2)), value from src;
insert into table acid_uami_n0 select cast(key as int), cast (key as decimal(5,2)), value from src;

-- insert overwrite
explain insert overwrite table acid_uami_n0 select cast(key as int), cast (key as decimal(5,2)), value
    from src order by cast(key as int) limit 10 ;
insert overwrite table acid_uami_n0 select cast(key as int), cast (key as decimal(5,2)), value
    from src order by cast(key as int) limit 10 ;

-- insert as select cont
explain insert into table acid_uami_n0 select cast(s1.key as int) as c1, cast (s2.key as decimal(5,2)) as c2, s1.value from src s1
    left outer join src s2 on s1.key=s2.key where s1.value > 'val' limit 10 ;
insert into table acid_uami_n0 select cast(s1.key as int) as c1, cast (s2.key as decimal(5,2)) as c2, s1.value from src s1
    left outer join src s2 on s1.key=s2.key where s1.value > 'val' limit 10 ;
select * from acid_uami_n0;
truncate table acid_uami_n0;

-- insert as select group by + agg
explain insert into table acid_uami_n0 select min(cast(key as int)) as c1, max(cast (key as decimal(5,2))) as c2, value
    from src group by key, value order by key, value limit 10;
insert into table acid_uami_n0 select min(cast(key as int)) as c1, max(cast (key as decimal(5,2))) as c2, value
    from src group by key, value order by key, value limit 10;
select * from acid_uami_n0;
truncate table acid_uami_n0;

-- multi insert
create table src_multi2_n0 (i STRING, j STRING NOT NULL ENABLE);
explain
from src
insert into table acid_uami_n0 select cast(key as int), cast(key as decimal(5,2)), value where key < 10
insert overwrite table src_multi2_n0 select * where key > 10 and key < 20;
drop table src_multi2_n0;

-- update
select * from acid_uami_n0 order by de desc limit 15;
explain update acid_uami_n0 set de = 893.14 where de = 103.00 or de = 119.00;
update acid_uami_n0 set de = 893.14 where de = 103.00 or de = 119.00;
select * from acid_uami_n0 order by de desc limit 15;
ALTER table acid_uami_n0 drop constraint ch2;
explain update acid_uami_n0 set vc = 'apache_hive' where de = 893.14 ;
update acid_uami_n0 set vc = 'apache_hive' where de = 893.14 ;
select * from acid_uami_n0 order by vc limit 15;
DROP TABLE acid_uami_n0;

-- MERGE
create table tmerge(key int CHECK (key > 0 AND (key < 100 OR key = 5)) enable, a1 string NOT NULL, value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");
DESC FORMATTED tmerge;

create table nonacid (key int, a1 string, value string) stored as orc;

-- with cardinality check off
set hive.merge.cardinality.check=false;
explain MERGE INTO tmerge as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

-- with cardinality check on
set hive.merge.cardinality.check=true;
explain MERGE INTO tmerge as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN MATCHED AND s.key < 3 THEN UPDATE set a1 = '1'
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

explain MERGE INTO tmerge as t using nonacid as s ON t.key = s.key
WHEN MATCHED AND s.key < 5 THEN DELETE
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, s.value);

DROP TABLE tmerge;
DROP TABLE nonacid;

-- drop constraint
CREATE TABLE numericDataType(a TINYINT CONSTRAINT tinyint_constraint DEFAULT 127Y ENABLE,
    b bigint CONSTRAINT check1 CHECK (b in(4,5)) ENABLE)
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED numericDataType;
ALTER TABLE numericDataType DROP CONSTRAINT check1;
DESC FORMATTED numericDataType;

EXPLAIN INSERT INTO numericDataType(b) values(456);
INSERT INTO numericDataType(b) values(456);
SELECT * from numericDataType;
DROP TABLE numericDataType;

-- column reference missing for column having check constraint
-- NULL for column with check shouldn't be possible
CREATE TABLE tcheck(a TINYINT, b bigint CONSTRAINT check1 CHECK (b in(4,5)) ENABLE)
    clustered by (b) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
DESC FORMATTED tcheck;
EXPLAIN INSERT INTO tcheck(a) values(1);
EXPLAIN INSERT INTO tcheck(b) values(4);
INSERT INTO tcheck(b) values(4);
SELECT * FROM tcheck;
DROP TABLE tcheck;

-- micro-managed table
set hive.create.as.insert.only=true;
set hive.exec.dynamic.partition.mode=nonstrict;
create table part_mm(key int check (key > 0 and key < 5000) enforced) partitioned by (key_mm int)
    stored as orc tblproperties ("transactional"="true", "transactional_properties"="insert_only");
explain insert into table part_mm partition(key_mm=455) select key from src order by value limit 3;
insert into table part_mm partition(key_mm=455) select key from src order by value desc limit 3;
select key from src order by value limit 3;
select * from part_mm;
drop table part_mm;

-- rely, novalidate
create table trely(i int);
ALTER TABLE trely CHANGE i i int CHECK (i>0) ENABLE NOVALIDATE RELY;
DESC FORMATTED trely;
DROP TABLE trely;

-- table level constraint
create table tbl1_n1(a string, b int, CONSTRAINT check1 CHECK (a != '' AND b > 4));
desc formatted tbl1_n1;
explain insert into tbl1_n1 values('a', 69);
insert into tbl1_n1 values('a', 69);
select * from tbl1_n1;
ALTER TABLE tbl1_n1 add constraint chk2 CHECK (b < 100);
desc formatted tbl1_n1;
explain insert into tbl1_n1 values('a', 69);
drop table tbl1_n1;
