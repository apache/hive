--! qt:replace:/(totalSize\s+)(\S+|\s+|.+)/$1#Masked#/
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
set hive.strict.checks.cartesian.product=false;

create table cmv_basetable_n100 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');
insert into cmv_basetable_n100 values
 (1, 'alfred', 10.30, 2),
 (2, 'bob', 3.14, 3),
 (2, 'bonnie', 172342.2, 3),
 (3, 'calvin', 978.76, 3),
 (3, 'charlie', 9.8, 1);

create table cmv_basetable_2_n100 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');
insert into cmv_basetable_2_n100 values
 (1, 'alfred', 10.30, 2),
 (3, 'calvin', 978.76, 3);

-- CREATE MATERIALIZED VIEW
CREATE MATERIALIZED VIEW cmv_mat_view_n300 AS
  SELECT cmv_basetable_n100.a, cmv_basetable_2_n100.c
  FROM cmv_basetable_n100 JOIN cmv_basetable_2_n100 ON (cmv_basetable_n100.a = cmv_basetable_2_n100.a)
  WHERE cmv_basetable_2_n100.c > 10.0
  GROUP BY cmv_basetable_n100.a, cmv_basetable_2_n100.c;

-- OUTDATED: NO
DESCRIBE FORMATTED cmv_mat_view_n300;

insert into cmv_basetable_2_n100 values
 (3, 'charlie', 15.8, 1);

-- OUTDATED: YES
DESCRIBE FORMATTED cmv_mat_view_n300;

-- REBUILD
ALTER MATERIALIZED VIEW cmv_mat_view_n300 REBUILD;

-- OUTDATED: NO
DESCRIBE FORMATTED cmv_mat_view_n300;

drop materialized view cmv_mat_view_n300;
drop table cmv_basetable_n100;
drop table cmv_basetable_2_n100;
