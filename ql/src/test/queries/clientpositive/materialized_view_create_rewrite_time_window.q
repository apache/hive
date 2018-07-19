SET hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

create table cmv_basetable_n3 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n3 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

analyze table cmv_basetable_n3 compute statistics for columns;

create table cmv_basetable_2_n1 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_2_n1 values
 (1, 'alfred', 10.30, 2),
 (3, 'calvin', 978.76, 3);

analyze table cmv_basetable_2_n1 compute statistics for columns;

-- CREATE VIEW WITH REWRITE DISABLED
EXPLAIN
CREATE MATERIALIZED VIEW cmv_mat_view_n3 DISABLE REWRITE TBLPROPERTIES('rewriting.time.window'='5min') AS
  SELECT cmv_basetable_n3.a, cmv_basetable_2_n1.c
  FROM cmv_basetable_n3 JOIN cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
  WHERE cmv_basetable_2_n1.c > 10.0
  GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

CREATE MATERIALIZED VIEW cmv_mat_view_n3 DISABLE REWRITE TBLPROPERTIES('rewriting.time.window'='5min') AS
  SELECT cmv_basetable_n3.a, cmv_basetable_2_n1.c
  FROM cmv_basetable_n3 JOIN cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
  WHERE cmv_basetable_2_n1.c > 10.0
  GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

DESCRIBE FORMATTED cmv_mat_view_n3;

-- CANNOT USE THE VIEW, IT IS DISABLED FOR REWRITE
EXPLAIN
SELECT cmv_basetable_n3.a
FROM cmv_basetable_n3 join cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
WHERE cmv_basetable_2_n1.c > 10.10
GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

SELECT cmv_basetable_n3.a
FROM cmv_basetable_n3 JOIN cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
WHERE cmv_basetable_2_n1.c > 10.10
GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

insert into cmv_basetable_2_n1 values
 (3, 'charlie', 15.8, 1);

analyze table cmv_basetable_2_n1 compute statistics for columns;

-- ENABLE FOR REWRITE
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n3 ENABLE REWRITE;

ALTER MATERIALIZED VIEW cmv_mat_view_n3 ENABLE REWRITE;

DESCRIBE FORMATTED cmv_mat_view_n3;

-- CAN USE THE MATERIALIZED VIEW, AS TIME WINDOW IS HUGE
-- WE GET OUTDATED RESULTS
EXPLAIN
SELECT cmv_basetable_n3.a
FROM cmv_basetable_n3 join cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
WHERE cmv_basetable_2_n1.c > 10.10
GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

SELECT cmv_basetable_n3.a
FROM cmv_basetable_n3 JOIN cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
WHERE cmv_basetable_2_n1.c > 10.10
GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

-- REBUILD
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n3 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n3 REBUILD;

DESCRIBE FORMATTED cmv_mat_view_n3;

-- CAN USE IT AGAIN
EXPLAIN
SELECT cmv_basetable_n3.a
FROM cmv_basetable_n3 join cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
WHERE cmv_basetable_2_n1.c > 10.10
GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

SELECT cmv_basetable_n3.a
FROM cmv_basetable_n3 JOIN cmv_basetable_2_n1 ON (cmv_basetable_n3.a = cmv_basetable_2_n1.a)
WHERE cmv_basetable_2_n1.c > 10.10
GROUP BY cmv_basetable_n3.a, cmv_basetable_2_n1.c;

drop materialized view cmv_mat_view_n3;
