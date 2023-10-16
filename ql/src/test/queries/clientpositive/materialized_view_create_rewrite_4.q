--! qt:replace:/(totalSize\s+)(\S+|\s+|.+)/$1#Masked#/
-- Test Incremental rebuild of materialized view with aggregate but without count(*)
-- when source tables have delete operations since last rebuild.
SET hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;
set hive.acid.direct.insert.enabled=false;

create table cmv_basetable_n5 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n5 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create table cmv_basetable_2_n2 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_2_n2 values
 (1, 'alfred', 10.30, 2),
 (3, 'calvin', 978.76, 3);

-- CREATE VIEW WITH REWRITE DISABLED
EXPLAIN
CREATE MATERIALIZED VIEW cmv_mat_view_n5 DISABLE REWRITE TBLPROPERTIES ('transactional'='true') AS
  SELECT cmv_basetable_n5.a, cmv_basetable_2_n2.c, sum(cmv_basetable_2_n2.d)
  FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
  WHERE cmv_basetable_2_n2.c > 10.0
  GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

CREATE MATERIALIZED VIEW cmv_mat_view_n5 DISABLE REWRITE TBLPROPERTIES ('transactional'='true') AS
  SELECT cmv_basetable_n5.a, cmv_basetable_2_n2.c, sum(cmv_basetable_2_n2.d)
  FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
  WHERE cmv_basetable_2_n2.c > 10.0
  GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

DESCRIBE FORMATTED cmv_mat_view_n5;

-- CANNOT USE THE VIEW, IT IS DISABLED FOR REWRITE
EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

insert into cmv_basetable_2_n2 values
 (3, 'charlie', 15.8, 1);

-- ENABLE FOR REWRITE
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n5 ENABLE REWRITE;

ALTER MATERIALIZED VIEW cmv_mat_view_n5 ENABLE REWRITE;

DESCRIBE FORMATTED cmv_mat_view_n5;

-- CANNOT USE THE VIEW, IT IS OUTDATED
EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

-- REBUILD
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

DESCRIBE FORMATTED cmv_mat_view_n5;

-- NOW IT CAN BE USED AGAIN
EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

-- NOW AN UPDATE
UPDATE cmv_basetable_2_n2 SET a=2 WHERE a=1;

-- INCREMENTAL REBUILD CANNOT BE TRIGGERED
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

DESCRIBE FORMATTED cmv_mat_view_n5;

-- MV CAN BE USED
EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

-- NOW A DELETE
DELETE FROM cmv_basetable_2_n2 WHERE a=2;

-- INCREMENTAL REBUILD CANNOT BE TRIGGERED
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

DESCRIBE FORMATTED cmv_mat_view_n5;

-- MV CAN BE USED
EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

-- NOW AN INSERT
insert into cmv_basetable_2_n2 values
 (1, 'charlie', 15.8, 1);

-- INCREMENTAL REBUILD CAN BE TRIGGERED AGAIN
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

DESCRIBE FORMATTED cmv_mat_view_n5;

-- MV CAN BE USED
EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a, cmv_basetable_2_n2.c;

drop materialized view cmv_mat_view_n5;
