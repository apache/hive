-- Test Incremental rebuild of materialized view without aggregate when source tables have
-- delete operations since last rebuild.
-- The view projects only one column.

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table cmv_basetable_n6 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n6 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create table cmv_basetable_2_n3 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_2_n3 values
 (1, 'alfred', 10.30, 2),
 (3, 'calvin', 978.76, 3);

CREATE MATERIALIZED VIEW cmv_mat_view_n6
  TBLPROPERTIES ('transactional'='true') AS
  SELECT cmv_basetable_n6.a
  FROM cmv_basetable_n6 JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
  WHERE cmv_basetable_2_n3.c > 10.0;

DELETE from cmv_basetable_2_n3 WHERE a=1;

ALTER MATERIALIZED VIEW cmv_mat_view_n6 REBUILD;

SELECT * FROM cmv_mat_view_n6;
