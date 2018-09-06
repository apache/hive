--! qt:dataset:src1
--! qt:dataset:src
set hive.mapred.mode=nonstrict;
set hive.auto.convert.join=false;

drop table if exists src_10;
drop table if exists src1_10;

create table src_10 as select * from src limit 2;
create table src1_10 as select * from src1 limit 2;

select key, count(*) from src1_10 group by key;
select key, count(*) from src_10 group by key;
-- Right Outer Join should be handled.

EXPLAIN
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src_10 x group by x.key) a
        RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1_10 y group by y.key) b
          ON (a.key = b.key)) tmp;
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src_10 x group by x.key) a
        RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1_10 y group by y.key) b
          ON (a.key = b.key)) tmp;

drop table if exists src_10;
drop table if exists src1_10;

create table src_10 as select * from src limit 3;
create table src1_10 as select * from src1 limit 3;

select key, count(*) from src1_10 group by key;
select key, count(*) from src_10 group by key;
-- Right Outer Join should be handled.

set hive.auto.convert.sortmerge.join=true;
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src_10 x group by x.key) a
        RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1_10 y group by y.key) b
          ON (a.key = b.key)) tmp;


drop table if exists src_10;
drop table if exists src1_10;

create table src_10 as select * from src limit 4;
create table src1_10 as select * from src1 limit 4;

select key, count(*) from src1_10 group by key;
select key, count(*) from src_10 group by key;
-- Right Outer Join should be handled.

set hive.auto.convert.sortmerge.join=true;
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src_10 x group by x.key) a
        RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1_10 y group by y.key) b
          ON (a.key = b.key)) tmp;

drop table if exists src_10;
drop table if exists src1_10;

create table src_10 as select * from src limit 5;
create table src1_10 as select * from src1 limit 5;

select key, count(*) from src1_10 group by key;
select key, count(*) from src_10 group by key;
-- Right Outer Join should be handled.

set hive.auto.convert.sortmerge.join=true;
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src_10 x group by x.key) a
        RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1_10 y group by y.key) b
          ON (a.key = b.key)) tmp;

drop table if exists src_10;
drop table if exists src1_10;

create table src_10 as select * from src limit 10;
create table src1_10 as select * from src1 limit 10;

select key, count(*) from src1_10 group by key;
select key, count(*) from src_10 group by key;
-- Right Outer Join should be handled.

set hive.auto.convert.sortmerge.join=true;
SELECT SUM(HASH(key1)), SUM(HASH(cnt1)), SUM(HASH(key2)), SUM(HASH(cnt2))
FROM (SELECT a.key AS key1, a.cnt AS cnt1, b.key AS key2, b.cnt AS cnt2
      FROM (SELECT x.key as key, count(x.value) AS cnt FROM src_10 x group by x.key) a
        RIGHT OUTER JOIN (SELECT y.key as key, count(y.value) AS cnt FROM src1_10 y group by y.key) b
          ON (a.key = b.key)) tmp;




CREATE TABLE t1 (c1 INT, c2 CHAR(100));
INSERT INTO t1 VALUES (1,''), (100,'abcdefghij'), (200, 'aa'), (300, 'bbb');

CREATE TABLE t2 (c1 INT);
INSERT INTO t2 VALUES (100), (200);

explain SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);
SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);

drop table t2;
CREATE TABLE t2 (c1 INT);
INSERT INTO t2 VALUES (100), (300);

explain SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);
SELECT c1 FROM t1 WHERE c1 NOT IN (SELECT c1 FROM t2 where t1.c1=t2.c1);

