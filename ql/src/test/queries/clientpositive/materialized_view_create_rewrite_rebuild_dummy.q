set hive.server2.materializedviews.registry.impl=DUMMY;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

create table cmv_basetable_n1 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n1 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create table cmv_basetable_2_n0 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_2_n0 values
 (1, 'alfred', 10.30, 2),
 (3, 'calvin', 978.76, 3);

EXPLAIN
CREATE MATERIALIZED VIEW cmv_mat_view_n1 AS
  SELECT cmv_basetable_n1.a, cmv_basetable_2_n0.c
  FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
  WHERE cmv_basetable_2_n0.c > 10.0
  GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

CREATE MATERIALIZED VIEW cmv_mat_view_n1 AS
  SELECT cmv_basetable_n1.a, cmv_basetable_2_n0.c
  FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
  WHERE cmv_basetable_2_n0.c > 10.0
  GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

-- USE THE VIEW
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

insert into cmv_basetable_2_n0 values
 (3, 'charlie', 15.8, 1);

-- CANNOT USE THE VIEW, IT IS OUTDATED
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

-- REBUILD
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n1 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n1 REBUILD;

-- NOW IT CAN BE USED AGAIN
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

DELETE FROM cmv_basetable_2_n0 WHERE a = 3;

-- CANNOT USE THE VIEW, IT IS OUTDATED
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

-- REBUILD
ALTER MATERIALIZED VIEW cmv_mat_view_n1 REBUILD;

-- NOW IT CAN BE USED AGAIN
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

-- IRRELEVANT OPERATIONS
create table cmv_irrelevant_table_n0 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_irrelevant_table_n0 values
 (1, 'alfred', 10.30, 2),
 (3, 'charlie', 9.8, 1);

-- IT CAN STILL BE USED
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

drop materialized view cmv_mat_view_n1;

-- NOT USED ANYMORE
EXPLAIN
SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 join cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;

SELECT cmv_basetable_n1.a
FROM cmv_basetable_n1 JOIN cmv_basetable_2_n0 ON (cmv_basetable_n1.a = cmv_basetable_2_n0.a)
WHERE cmv_basetable_2_n0.c > 10.10
GROUP BY cmv_basetable_n1.a, cmv_basetable_2_n0.c;
