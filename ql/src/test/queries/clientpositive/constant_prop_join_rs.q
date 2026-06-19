set hive.llap.io.enabled=false;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=10;

drop table if exists t0;
drop table if exists t1;
drop table if exists t2;
drop table if exists t3;

create table if not exists t0 (c00 int, c01 int, c03 TIMESTAMP) PARTITIONED BY (c02 string) stored as orc;
create table if not exists t1 (c10 int, c11 int, c12 int) PARTITIONED BY (c13 string) stored as orc;

create table if not exists t2 (c20 int) PARTITIONED BY (c21 string) stored as orc;
create table if not exists t3 (c30 TIMESTAMP) PARTITIONED BY (c31 string) stored as orc;


alter table t0 add partition(c02='test0');
alter table t1 add partition(c13='test1');
alter table t2 add partition(c21='test1');
alter table t3 add partition(c31='test2');


alter table t0 partition(c02='test0') update statistics set('numRows'='153373500','rawDataSize'='2053794707568');
alter table t1 partition(c13='test1') update statistics set('numRows'='1250000','rawDataSize'='2700000000');
alter table t2 partition(c21='test1') update statistics set('numRows'='475011','rawDataSize'='641987831');
alter table t3 partition(c31='test2') update statistics set('numRows'='136672296','rawDataSize'='141045810480');


set hive.explain.user=false;
set hive.cbo.enable=false;
set hive.join.inner.residual=false;

explain SELECT t0.c00 FROM t0
JOIN t1 ON (t0.c00 = t1.c10 AND t0.c01 BETWEEN t1.c11 AND t1.c12)
LEFT OUTER JOIN t2 ON ( t1.c13 = t2.c21)
LEFT OUTER JOIN
  (SELECT c30 FROM t3) s0 ON datediff (CURRENT_TIMESTAMP, t0.c03) = datediff (CURRENT_TIMESTAMP, s0.c30)
WHERE t1.c13 = 'test1';

SELECT t0.c00 FROM t0
JOIN t1 ON (t0.c00 = t1.c10 AND t0.c01 BETWEEN t1.c11 AND t1.c12)
LEFT OUTER JOIN t2 ON ( t1.c13 = t2.c21)
LEFT OUTER JOIN
  (SELECT c30 FROM t3) s0 ON datediff (CURRENT_TIMESTAMP, t0.c03) = datediff (CURRENT_TIMESTAMP, s0.c30)
WHERE t1.c13 = 'test1';
