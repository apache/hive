-- SORT_QUERY_RESULTS

SET hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;
set hive.materializedview.rewriting=true;

CREATE TABLE cmv_basetable_n2
STORED AS orc
TBLPROPERTIES ('transactional'='true')
AS
SELECT cast(current_timestamp() AS timestamp) AS t,
       cast(a AS int) AS a,
       cast(b AS varchar(256)) AS b,
       cast(userid AS varchar(256)) AS userid,
       cast(c AS double) AS c,
       cast(d AS int) AS d
FROM TABLE (
  VALUES
    (1, 'alfred', 'alfred', 10.30, 2),
    (2, 'bob', 'bob', 3.14, 3),
    (2, 'bonnie', 'bonnie', 172342.2, 3),
    (3, 'calvin', 'calvin', 978.76, 3),
    (3, 'charlie', 'charlie_a', 9.8, 1),
    (3, 'charlie', 'charlie_b', 15.8, 1)) as q (a, b, userid, c, d);

CREATE MATERIALIZED VIEW cmv_mat_view_n2 ENABLE REWRITE
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR")
AS
SELECT cast(t AS timestamp with local time zone) as `__time`, a, b, c, userid
FROM cmv_basetable_n2
WHERE a = 2;

SELECT a, b, c FROM cmv_mat_view_n2;

SHOW TBLPROPERTIES cmv_mat_view_n2;

CREATE MATERIALIZED VIEW IF NOT EXISTS cmv_mat_view2_n0 ENABLE REWRITE
STORED BY 'org.apache.hadoop.hive.druid.DruidStorageHandler'
TBLPROPERTIES ("druid.segment.granularity" = "HOUR")
AS
SELECT cast(t AS timestamp with local time zone) as `__time`, a, b, c, userid
FROM cmv_basetable_n2
WHERE a = 3;

SELECT a, c FROM cmv_mat_view2_n0;

SHOW TBLPROPERTIES cmv_mat_view2_n0;

EXPLAIN
SELECT a, c
FROM cmv_basetable_n2
WHERE a = 3;

SELECT a, c
FROM cmv_basetable_n2
WHERE a = 3;

EXPLAIN
SELECT * FROM (
  (SELECT a, c FROM cmv_basetable_n2 WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable_n2 WHERE d = 3) table2
  ON table1.a = table2.a);

SELECT * FROM (
  (SELECT a, c FROM cmv_basetable_n2 WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable_n2 WHERE d = 3) table2
  ON table1.a = table2.a);

INSERT INTO cmv_basetable_n2 VALUES
 (cast(current_timestamp() AS timestamp), 3, 'charlie', 'charlie_c', 15.8, 1);

-- TODO: CANNOT USE THE VIEW, IT IS OUTDATED
EXPLAIN
SELECT * FROM (
  (SELECT a, c FROM cmv_basetable_n2 WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable_n2 WHERE d = 3) table2
  ON table1.a = table2.a);

SELECT * FROM (
  (SELECT a, c FROM cmv_basetable_n2 WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable_n2 WHERE d = 3) table2
  ON table1.a = table2.a);

-- REBUILD
EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view2_n0 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view2_n0 REBUILD;

SHOW TBLPROPERTIES cmv_mat_view2_n0;

-- NOW IT CAN BE USED AGAIN
EXPLAIN
SELECT * FROM (
  (SELECT a, c FROM cmv_basetable_n2 WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable_n2 WHERE d = 3) table2
  ON table1.a = table2.a);

SELECT * FROM (
  (SELECT a, c FROM cmv_basetable_n2 WHERE a = 3) table1
  JOIN
  (SELECT a, c FROM cmv_basetable_n2 WHERE d = 3) table2
  ON table1.a = table2.a);

DROP MATERIALIZED VIEW cmv_mat_view_n2;
DROP MATERIALIZED VIEW cmv_mat_view2_n0;
DROP TABLE cmv_basetable_n2;
