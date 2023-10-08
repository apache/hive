set hive.cbo.fallback.strategy=NEVER;

create table tbl1 (key int, f1 int);
create table tbl2 (f1 int) partitioned by (key int);

FROM (select key, f1 FROM tbl1 where key=5) a
INSERT OVERWRITE TABLE tbl2 partition(key=5)
select f1 WHERE key > 0 GROUP by f1
INSERT OVERWRITE TABLE tbl2 partition(key=6)
select f1 WHERE key > 0 GROUP by f1;
