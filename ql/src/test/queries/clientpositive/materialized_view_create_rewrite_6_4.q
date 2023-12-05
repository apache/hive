SET hive.vectorized.execution.enabled=false;
set hive.support.concurrency=true;
set hive.txn.manager=org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
--set hive.strict.checks.cartesian.product=false;
--set hive.materializedview.rewriting=true;

create table cmv_basetable_n6 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');

insert into cmv_basetable_n6 values
(1, 'alfred', 10.30, 2),
(1, 'charlie', 20.30, 2);

create table cmv_basetable_2_n3 (a int, b varchar(256), c decimal(10,2), d int) stored as orc TBLPROPERTIES ('transactional'='true');


insert into cmv_basetable_2_n3 values
(1, 'bob', 30.30, 2),
(1, 'bonnie', 40.30, 2);

create table t3 (a int, b varchar(256), c decimal(10,2)) stored as orc TBLPROPERTIES ('transactional'='true');

insert into t3 values
(1, 'foo', 30.30),
(1, 'bar', 30.30);

CREATE MATERIALIZED VIEW mat1 TBLPROPERTIES ('transactional'='true') AS
SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_n6.ROW__ID r1, cmv_basetable_2_n3.ROW__ID r2, t3.ROW__ID r3
FROM cmv_basetable_n6
JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
JOIN t3 ON (t3.a = cmv_basetable_2_n3.a)
WHERE cmv_basetable_n6.c > 10 OR cmv_basetable_2_n3.c;

select * from mat1;

insert into cmv_basetable_n6 values
(1, 'kevin', 50.30, 2);

insert into t3 values
(1, 'new rec', 60.30);

DELETE FROM cmv_basetable_2_n3 WHERE b = 'bonnie';


SELECT cmv_basetable_n6.ROW__IS__DELETED, cmv_basetable_n6.ROW__ID.writeId, cmv_basetable_n6.a, cmv_basetable_2_n3.ROW__IS__DELETED, cmv_basetable_2_n3.ROW__ID.writeId, cmv_basetable_2_n3.c, t3.ROW__IS__DELETED, t3.ROW__ID.writeId
FROM cmv_basetable_n6('acid.fetch.deleted.rows'='true')
JOIN cmv_basetable_2_n3('acid.fetch.deleted.rows'='true') ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
JOIN t3('acid.fetch.deleted.rows'='true') ON (t3.a = cmv_basetable_2_n3.a)
where
(cmv_basetable_n6.ROW__ID.writeId > 1 or cmv_basetable_2_n3.ROW__ID.writeId > 1 or t3.ROW__ID.writeId > 1)
and not (
  cmv_basetable_n6.ROW__ID.writeId > 1 and cmv_basetable_2_n3.ROW__ID.writeId > 1 and t3.ROW__ID.writeId > 1 and
  ((cmv_basetable_n6.ROW__IS__DELETED or cmv_basetable_2_n3.ROW__IS__DELETED or t3.ROW__IS__DELETED) and (not cmv_basetable_n6.ROW__IS__DELETED or not cmv_basetable_2_n3.ROW__IS__DELETED or not t3.ROW__IS__DELETED))
);



EXPLAIN CBO
ALTER MATERIALIZED VIEW mat1 REBUILD;

EXPLAIN
ALTER MATERIALIZED VIEW mat1 REBUILD;

ALTER MATERIALIZED VIEW mat1 REBUILD;

select * from mat1;

drop materialized view mat1;

SELECT cmv_basetable_n6.a, cmv_basetable_2_n3.c, cmv_basetable_n6.ROW__ID r1, cmv_basetable_2_n3.ROW__ID r2, t3.ROW__ID r3
FROM cmv_basetable_n6
JOIN cmv_basetable_2_n3 ON (cmv_basetable_n6.a = cmv_basetable_2_n3.a)
JOIN t3 ON (t3.a = cmv_basetable_2_n3.a)
WHERE cmv_basetable_n6.c > 10 OR cmv_basetable_2_n3.c;
