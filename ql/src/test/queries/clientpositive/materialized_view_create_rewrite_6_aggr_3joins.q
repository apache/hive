-- Test Incremental rebuild of materialized view with aggregate and count(*) and 3 joined tables
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

create table t3 (a int, b varchar(256), c decimal(10,2)) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t3 values
(1, 'foo', 30.30),
(1, 'bar', 30.30),
(2, 'bar', 30.30);

CREATE MATERIALIZED VIEW mat1 TBLPROPERTIES ('transactional'='true') AS
SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, count(*)
FROM cmv_basetable_n6
JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
JOIN t3 ON (t3.a = cmv_basetable_2_n3.a)
WHERE cmv_basetable_n6.c > 10 OR cmv_basetable_2_n3.c > 10
group by cmv_basetable_n6.a, cmv_basetable_2_n3.c;

insert into cmv_basetable_n6 values
(1, 'kevin', 50.30, 2);

insert into t3 values
(1, 'new rec', 60.30);

DELETE FROM cmv_basetable_2_n3 WHERE b = 'bonnie';


EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;

ALTER MATERIALIZED VIEW mat1 REBUILD;

select * from mat1;


-- Delete only from one table, do not change the rest of the tables
delete from cmv_basetable_n6 where b = 'kevin';

EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;

ALTER MATERIALIZED VIEW mat1 REBUILD;

select * from mat1;


drop materialized view mat1;

SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, count(*)
FROM cmv_basetable_n6
JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
JOIN t3 ON (t3.a = cmv_basetable_2_n3.a)
WHERE cmv_basetable_n6.c > 10 OR cmv_basetable_2_n3.c > 10
group by cmv_basetable_n6.a, cmv_basetable_2_n3.c;
