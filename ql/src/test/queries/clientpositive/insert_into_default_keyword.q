set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
-- SORT_QUERY_RESULTS

DROP TABLE insert_into1_n0;

-- No default constraint
CREATE TABLE insert_into1_n0 (key int, value string)
    clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

EXPLAIN INSERT INTO TABLE insert_into1_n0 values(default, DEFAULT);
INSERT INTO TABLE insert_into1_n0 values(default, DEFAULT);
SELECT * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

-- should be able to use any case for DEFAULT
EXPLAIN INSERT INTO TABLE insert_into1_n0 values(234, dEfAULt);
INSERT INTO TABLE insert_into1_n0 values(234, dEfAULt);
SELECT * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

-- multi values
explain insert into insert_into1_n0 values(default, 3),(2,default);
insert into insert_into1_n0 values(default, 3),(2,default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

--with column schema
EXPLAIN INSERT INTO TABLE insert_into1_n0(key) values(default);
INSERT INTO TABLE insert_into1_n0(key) values(default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

EXPLAIN INSERT INTO TABLE insert_into1_n0(key, value) values(2,default);
INSERT INTO TABLE insert_into1_n0(key, value) values(2,default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

DROP TABLE insert_into1_n0;

-- with default constraint
CREATE TABLE insert_into1_n0 (key int DEFAULT 1, value string)
    clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
EXPLAIN INSERT INTO TABLE insert_into1_n0 values(default, DEFAULT);
INSERT INTO TABLE insert_into1_n0 values(default, DEFAULT);
SELECT * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

-- with default complex constraint
CREATE TABLE insert_into1_n1 (key int, value string DEFAULT cast(round(round(1.245, 2), 1) as string))
     clustered by (key) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');
EXPLAIN INSERT INTO TABLE insert_into1_n1 values(default, DEFAULT);
INSERT INTO TABLE insert_into1_n1 values(default, DEFAULT);
SELECT * from insert_into1_n1;
TRUNCATE table insert_into1_n1;

-- should be able to use any case for DEFAULT
EXPLAIN INSERT INTO TABLE insert_into1_n0 values(234, dEfAULt);
INSERT INTO TABLE insert_into1_n0 values(234, dEfAULt);
SELECT * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

-- multi values
explain insert into insert_into1_n0 values(default, 3),(2,default);
insert into insert_into1_n0 values(default, 3),(2,default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

--with column schema
EXPLAIN INSERT INTO TABLE insert_into1_n0(key) values(default);
INSERT INTO TABLE insert_into1_n0(key) values(default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

EXPLAIN INSERT INTO TABLE insert_into1_n0(key, value) values(2,default);
INSERT INTO TABLE insert_into1_n0(key, value) values(2,default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

EXPLAIN INSERT INTO TABLE insert_into1_n0(value, key) values(2,default);
INSERT INTO TABLE insert_into1_n0(value, key) values(2,default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

EXPLAIN INSERT INTO TABLE insert_into1_n0(key, value) values(2,default),(DEFAULT, default);
INSERT INTO TABLE insert_into1_n0(key, value) values(2,default),(DEFAULT, default);
select * from insert_into1_n0;
TRUNCATE table insert_into1_n0;
DROP TABLE insert_into1_n0;


-- UPDATE
CREATE TABLE insert_into1_n0 (key int DEFAULT 1, value string, i int)
    clustered by (i) into 2 buckets stored as orc TBLPROPERTIES ('transactional'='true');

INSERT INTO insert_into1_n0 values(2,1, 45);
EXPLAIN UPDATE insert_into1_n0 set key = DEFAULT where value=1;
UPDATE insert_into1_n0 set key = DEFAULT where value=1;
SELECT * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

INSERT INTO insert_into1_n0 values(2,1, 45);
EXPLAIN UPDATE insert_into1_n0 set key = DEFAULT, value=DEFAULT where value=1;
UPDATE insert_into1_n0 set key = DEFAULT, value=DEFAULT where value=1;
SELECT * from insert_into1_n0;
TRUNCATE table insert_into1_n0;

DROP TABLE insert_into1_n0;

-- partitioned table
CREATE TABLE tpart(i int, j int DEFAULT 1001) partitioned by (ds string);
-- no column schema
EXPLAIN INSERT INTO tpart partition(ds='1') values(DEFAULT, DEFAULT);
INSERT INTO tpart partition(ds='1') values(DEFAULT, DEFAULT);
SELECT * FROM tpart;
TRUNCATE table tpart;
-- with column schema
EXPLAIN INSERT INTO tpart partition(ds='1')(i) values(DEFAULT);
INSERT INTO tpart partition(ds='1')(i) values(DEFAULT);
EXPLAIN INSERT INTO tpart partition(ds='1')(i,j) values(10, DEFAULT);
INSERT INTO tpart partition(ds='1')(i,j) values(10, DEFAULT);
SELECT * FROM tpart;
TRUNCATE table tpart;
DROP TABLE tpart;

-- MEREGE
set hive.mapred.mode=nonstrict;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table nonacid_n1 (key int, a1 string, value string) stored as orc;
insert into nonacid_n1 values(1, 'a11', 'val1');
insert into nonacid_n1 values(2, 'a12', 'val2');

create table acidTable(key int NOT NULL enable, a1 string DEFAULT 'a1', value string)
clustered by (value) into 2 buckets stored as orc
tblproperties ("transactional"="true");
insert into acidTable values(1, 'a10','val100');

-- only insert
explain MERGE INTO acidTable as t using nonacid_n1 as s ON t.key = s.key
WHEN NOT MATCHED THEN INSERT VALUES (s.key, DEFAULT, DEFAULT);

MERGE INTO acidTable as t using nonacid_n1 as s ON t.key = s.key
WHEN NOT MATCHED THEN INSERT VALUES (s.key, DEFAULT, DEFAULT);
select * from acidTable;
truncate table acidTable;
insert into acidTable values(1, 'a10','val100');

-- insert + update + delete
explain MERGE INTO acidTable as t using nonacid_n1 as s ON t.key = s.key
WHEN MATCHED AND s.key < 3 THEN DELETE
WHEN MATCHED AND s.key > 3 THEN UPDATE set a1 = DEFAULT
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, DEFAULT);
MERGE INTO acidTable as t using nonacid_n1 as s ON t.key = s.key
WHEN MATCHED AND s.key < 3 THEN DELETE
WHEN MATCHED AND s.key > 3 THEN UPDATE set a1 = DEFAULT
WHEN NOT MATCHED THEN INSERT VALUES (s.key, s.a1, DEFAULT);
select * from acidTable;
truncate table acidTable;

create table acidTable2(key int DEFAULT 404) clustered by (key) into 2 buckets stored as orc
tblproperties ("transactional"="true");

explain MERGE INTO acidTable2 as t using nonacid_n1 as s ON t.key = s.key
WHEN NOT MATCHED THEN INSERT VALUES (DEFAULT);
MERGE INTO acidTable2 as t using nonacid_n1 as s ON t.key = s.key
WHEN NOT MATCHED THEN INSERT VALUES (DEFAULT);
select * from acidTable2;

DROP TABLE acidTable;
DROP TABLE acidTable2;
DROP TABLE nonacid_n1;
