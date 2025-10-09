-- Test Incremental rebuild of materialized view with aggregate and count(*) and two joined tables
-- when records is deleted from one source table and another is inserted into the other table with the same join key values.

set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;

create table cmv_basetable_n6 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n6 values
(1, 'alfred', 10.30, 2),
(1, 'charlie', 20.30, 2),
(2, 'zoe', 100.30, 2);

create table cmv_basetable_2_n3 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_2_n3 values
(1, 'bob', 30.30, 2),
(1, 'bonnie', 40.30, 2),
(2, 'joe', 130.30, 2);

SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_2_n3.b, count(*)
FROM cmv_basetable_n6 JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
group by cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_2_n3.b;

CREATE MATERIALIZED VIEW cmv_mat_view_n6 TBLPROPERTIES ('transactional'='true') AS
SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_2_n3.b, count(*)
FROM cmv_basetable_n6 JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
group by cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_2_n3.b;

insert into cmv_basetable_n6 values
(1, 'kevin', 50.30, 2);

DELETE FROM cmv_basetable_2_n3 WHERE b = 'bonnie';

EXPLAIN CBO
ALTER MATERIALIZED VIEW cmv_mat_view_n6 REBUILD;

EXPLAIN
ALTER MATERIALIZED VIEW cmv_mat_view_n6 REBUILD;

ALTER MATERIALIZED VIEW cmv_mat_view_n6 REBUILD;

select * from cmv_mat_view_n6;

drop materialized view cmv_mat_view_n6;

SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_2_n3.b, count(*)
FROM cmv_basetable_n6 JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
group by cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_2_n3.b;
