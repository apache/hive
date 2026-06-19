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

CREATE MATERIALIZED VIEW cmv_mat_view_n5 TBLPROPERTIES ('transactional'='true') AS
  SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
  FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
  WHERE cmv_basetable_2_n2.c > 10.0
  GROUP BY cmv_basetable_n5.a;

insert into cmv_basetable_2_n2 values
 (3, 'charlie', 15.8, 1);

EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n5 REBUILD;

EXPLAIN
SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 join cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a;

SELECT cmv_basetable_n5.a, sum(cmv_basetable_2_n2.d)
FROM cmv_basetable_n5 JOIN cmv_basetable_2_n2 ON (cmv_basetable_n5.a = cmv_basetable_2_n2.a)
WHERE cmv_basetable_2_n2.c > 10.10
GROUP BY cmv_basetable_n5.a;

drop materialized view cmv_mat_view_n5;
