-- SORT_QUERY_RESULTS

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

CREATE TABLE cmv_basetable
STORED AS orc
TBLPROPERTIES ('transactional'='true')
AS
SELECT cast(current_timestamp() AS timestamp) AS t,
       cast(a AS int) AS a,
       cast(b AS varchar(256)) AS b,
       cast(c AS double) AS c,
       cast(d AS int) AS d
FROM TABLE (
  VALUES
    (1, 'alfred', 10.30, 2),
    (2, 'bob', 3.14, 3),
    (2, 'bonnie', 172342.2, 3),
    (3, 'calvin', 978.76, 3),
    (3, 'charlie', 9.8, 1),
    (3, 'charlie', 15.8, 1)) as q (a, b, c, d);

CREATE MATERIALIZED VIEW cmv_mat_view ENABLE REWRITE
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR")
AS
SELECT cast(t AS timestamp with local time zone) as `__time`, a, b, c
FROM cmv_basetable
WHERE a = 2;

SELECT a, b, c FROM cmv_mat_view;

SHOW TBLPROPERTIES cmv_mat_view;

CREATE MATERIALIZED VIEW IF NOT EXISTS cmv_mat_view2 ENABLE REWRITE
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR")
AS
SELECT cast(t AS timestamp with local time zone) as `__time`, a, b, c
FROM cmv_basetable
WHERE a = 3;

SELECT a, c FROM cmv_mat_view2;

SHOW TBLPROPERTIES cmv_mat_view2;

EXPLAIN
SELECT a, c
FROM cmv_basetable
WHERE a = 3;

SELECT a, c
FROM cmv_basetable
WHERE a = 3;

EXPLAIN
SELECT * FROM (
  (SELECT a, c FROM cmv_basetable WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable WHERE d = 3) table2
  ON table1.a = table2.a);

SELECT * FROM (
  (SELECT a, c FROM cmv_basetable WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable WHERE d = 3) table2
  ON table1.a = table2.a);

INSERT INTO cmv_basetable VALUES
 (cast(current_timestamp() AS timestamp), 3, 'charlie', 15.8, 1);

-- TODO: CANNOT USE THE VIEW, IT IS OUTDATED
EXPLAIN
SELECT * FROM (
  (SELECT a, c FROM cmv_basetable WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable WHERE d = 3) table2
  ON table1.a = table2.a);

SELECT * FROM (
  (SELECT a, c FROM cmv_basetable WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable WHERE d = 3) table2
  ON table1.a = table2.a);

-- REBUILD
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view2 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view2 REBUILD;

SHOW TBLPROPERTIES cmv_mat_view2;

-- NOW IT CAN BE USED AGAIN
EXPLAIN
SELECT * FROM (
  (SELECT a, c FROM cmv_basetable WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable WHERE d = 3) table2
  ON table1.a = table2.a);

SELECT * FROM (
  (SELECT a, c FROM cmv_basetable WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable WHERE d = 3) table2
  ON table1.a = table2.a);

DROP MATERIALIZED VIEW cmv_mat_view;
DROP MATERIALIZED VIEW cmv_mat_view2;
DROP TABLE cmv_basetable;
